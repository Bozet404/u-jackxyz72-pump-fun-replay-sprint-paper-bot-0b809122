#!/usr/bin/env python3
"""Quiet-alive paper bot for the Pump.fun Replay Sprint (make.fun).

The house replay scorer feeds this bot a stream of pump.fun bonding-curve
events over stdin as newline-delimited JSON (protocol agent-fun/replay/v2):
a `hello` message first, then one `batch` per slot window, then `done`.
For every batch the bot prints exactly one `reply` line with its orders.

Strategy, in plain words: only buy a coin that is alive but has not pumped
yet, and get out on a schedule. On pump.fun most new coins spike on their
first buys and then bleed back down, and a 2-slot execution latency means a
bot that chases the spike buys the top. So this bot waits: a coin becomes a
candidate only when it is 1000 to 4000 slots old, its price is still within
10% of its creation price, it has taken at least 0.5 SOL of buys across at
least 3 buys (real activity, not one insider print), and the order-size cap
(1% of its real SOL reserve) allows a 0.01 SOL buy. It buys min(0.05 SOL,
the cap), one buy per coin, at most 5 open positions. It sells the whole
balance when the mark doubles (+100%), falls 30%, or the position is 2000
slots old, checked for every open position on every batch. A completed
(graduated) curve is never bought, and a position on one is left for the
replay to close.

Everything is deterministic: integer cross-multiplication for the price
tests, no clock, no randomness, no floats, so two runs over the same replay
produce the same replies byte for byte.

All amounts are lamports (u64, decimal strings on the wire). Fees come from
the hello message (base + priority per transaction; protocol + creator bps
are priced into the curve itself and need no separate accounting here).
"""

import json
import sys

LAMPORTS_PER_SOL = 1_000_000_000

# --- Tunable strategy parameters (calibrated on the kit's dev window) ---
STAKE_LAMPORTS = 50_000_000       # 0.05 SOL: target size of each buy
MIN_BUY_LAMPORTS = 10_000_000     # 0.01 SOL: smallest buy that counts as a
                                  # round trip toward the 20-coin guardrail
MAX_SLIPPAGE_BPS = 10_000         # wide open: never refuse a fill for the
                                  # price moving during the 2-slot latency
MIN_AGE_SLOTS = 1000              # candidate window: coin age in slots
MAX_AGE_SLOTS = 4000
RISE_BAND_BPS = 1000              # price within +/-10% of creation price
MIN_BUY_VOLUME_LAMPORTS = 500_000_000  # 0.5 SOL of buys before entry
MIN_BUY_COUNT = 3                 # ... across at least 3 buys
TAKE_PROFIT_NUM = 2               # sell at +100%: vs * evt >= 2 * evs * vt
TAKE_PROFIT_DEN = 1
STOP_LOSS_NUM = 7                 # sell at -30%: 10 * vs * evt <= 7 * evs * vt
STOP_LOSS_DEN = 10
HOLD_TIMEOUT_SLOTS = 2000         # sell whatever is left after this many slots


class QuietAliveBot:
    """State for one replay run. All decisions are functions of the messages
    seen so far, in stream order, so the run is fully deterministic."""

    def __init__(self):
        self.capital_lamports = 10 * LAMPORTS_PER_SOL
        self.max_open_positions = 5
        self.max_orders_per_batch = 16
        self.max_order_bps = 100
        self.tx_fee_lamports = 105_000
        # mint -> (virtual_sol, virtual_token, real_sol) latest seen
        self.curves = {}
        # mint -> [creation_slot, creation_vs, creation_vt,
        #          buy_volume_lamports, buy_count]
        self.coin_stats = {}
        # mints whose curve completed (graduated): never buy, never chase
        self.completed = set()
        # mints already bought (one buy per coin, ever)
        self.decided = set()
        # mint -> (entry_virtual_sol, entry_virtual_token, buy_slot)
        self.entries = {}
        # client_id -> (mint, quote_lamports) for buys not yet reported
        self.pending_buys = {}
        # client_id -> mint for sells not yet reported
        self.pending_sells = {}
        self.cash_lamports = 0
        # mint -> token_amount, from the latest account snapshot
        self.positions = {}
        self.buy_counter = 0
        self.sell_counter = 0

    # -- hello -----------------------------------------------------------
    def on_hello(self, msg):
        self.capital_lamports = int(msg["capital"]["lamports"])
        caps = msg["caps"]
        self.max_open_positions = int(caps["max_open_positions"])
        self.max_orders_per_batch = int(caps["max_orders_per_batch"])
        self.max_order_bps = int(caps["max_order_bps_of_real_reserve"])
        fees = msg["fees"]
        self.tx_fee_lamports = int(fees["base_fee_lamports"]) + int(
            fees["priority_fee_lamports"]
        )
        self.cash_lamports = self.capital_lamports

    # -- per-batch bookkeeping -------------------------------------------
    def apply_fills(self, fills):
        """Drop in-flight orders the harness has decided, whatever the outcome."""
        for fill in fills or []:
            self.pending_buys.pop(fill["client_id"], None)
            self.pending_sells.pop(fill["client_id"], None)

    def apply_account(self, account, slot):
        self.cash_lamports = int(account["cash_lamports"])
        held_now = {}
        for pos in account["positions"]:
            held_now[pos["mint"]] = int(pos["token_amount"])
        self.positions = held_now
        # Forget entry prices for positions that are gone (sold, completed,
        # or flattened by the replay). A position that appears with no entry
        # record (never expected: every position came from one of our buys)
        # is seeded at the current mark so it can still be exited.
        for mint in list(self.entries):
            if mint not in held_now:
                del self.entries[mint]
        for mint in held_now:
            if mint not in self.entries:
                curve = self.curves.get(mint)
                if curve is not None and curve[1] != 0:
                    self.entries[mint] = (curve[0], curve[1], slot)

    def apply_marks(self, marks):
        for mark in marks or []:
            mint = mark["mint"]
            self.curves[mint] = (
                int(mark["virtual_quote_reserves"]),
                int(mark["virtual_token_reserves"]),
                int(mark["real_quote_reserves"]),
            )

    def apply_events(self, events):
        """Fold a batch's events into curve and per-coin activity state."""
        for event in events or []:
            kind = event["event"]
            mint = event["mint"]
            if kind == "CreateEvent":
                self.curves[mint] = (
                    int(event["virtual_sol_reserves"]),
                    int(event["virtual_token_reserves"]),
                    0,
                )
                self.coin_stats[mint] = [
                    int(event["slot"]),
                    int(event["virtual_sol_reserves"]),
                    int(event["virtual_token_reserves"]),
                    0,
                    0,
                ]
            elif kind == "TradeEvent":
                self.curves[mint] = (
                    int(event["virtual_sol_reserves"]),
                    int(event["virtual_token_reserves"]),
                    int(event["real_sol_reserves"]),
                )
                stats = self.coin_stats.get(mint)
                if stats is not None and event["is_buy"]:
                    stats[3] += int(event["sol_amount"])
                    stats[4] += 1
            elif kind == "CompleteEvent":
                self.completed.add(mint)

    # -- decisions ---------------------------------------------------------
    def size_cap_lamports(self, mint):
        """Largest buy the harness allows for this mint right now."""
        curve = self.curves.get(mint)
        if curve is None:
            return 0
        real_sol = curve[2]
        return real_sol * self.max_order_bps // 10_000

    def committed_positions(self):
        """Open positions counting buys still in flight."""
        return len(self.positions) + len(self.pending_buys)

    def free_cash_lamports(self):
        """Cash left after reserving for orders already sent."""
        reserved = 0
        for _cid, (_mint, quote) in self.pending_buys.items():
            reserved += quote + self.tx_fee_lamports
        for _cid in self.pending_sells:
            reserved += self.tx_fee_lamports
        free = self.cash_lamports - reserved
        return free if free > 0 else 0

    def is_quiet_alive(self, mint, slot):
        """True when the coin is old enough to have shown its character,
        still near its creation price, and has real buy activity behind it."""
        stats = self.coin_stats.get(mint)
        if stats is None:
            return False
        slot0, vs0, vt0, buy_volume, buy_count = stats
        age = slot - slot0
        if age < MIN_AGE_SLOTS or age > MAX_AGE_SLOTS:
            return False
        if buy_volume < MIN_BUY_VOLUME_LAMPORTS or buy_count < MIN_BUY_COUNT:
            return False
        curve = self.curves.get(mint)
        if curve is None or curve[1] == 0 or vt0 == 0:
            return False
        vs, vt, _rs = curve
        # Rise within +/-RISE_BAND_BPS of creation, in exact integers:
        # 0.9 <= (vs * vt0) / (vt * vs0) <= 1.1
        left = vs * vt0
        right = vt * vs0
        band = RISE_BAND_BPS
        if 10_000 * left < (10_000 - band) * right:
            return False
        if 10_000 * left > (10_000 + band) * right:
            return False
        return True

    def exit_orders(self, slot):
        """Sell orders for every open position that hit take-profit,
        stop-loss, or the hold timeout. Sorted by mint so the reply order
        never depends on arrival order."""
        orders = []
        for mint in sorted(self.positions):
            if mint in self.pending_sells.values():
                continue
            entry = self.entries.get(mint)
            if entry is None:
                continue
            evs, evt, buy_slot = entry
            curve = self.curves.get(mint)
            if curve is None:
                continue
            vs, vt, _rs = curve
            if vt == 0 or evt == 0:
                continue
            take_profit = (
                TAKE_PROFIT_DEN * vs * evt >= TAKE_PROFIT_NUM * evs * vt
            )
            stop_loss = (
                STOP_LOSS_DEN * vs * evt <= STOP_LOSS_NUM * evs * vt
            )
            timed_out = slot - buy_slot >= HOLD_TIMEOUT_SLOTS
            if not (take_profit or stop_loss or timed_out):
                continue
            self.sell_counter += 1
            client_id = "s%d" % self.sell_counter
            self.pending_sells[client_id] = mint
            orders.append(
                {
                    "side": "sell",
                    "client_id": client_id,
                    "mint": mint,
                    "token_amount": str(self.positions[mint]),
                    "max_slippage_bps": MAX_SLIPPAGE_BPS,
                }
            )
        return orders

    def entry_orders(self, slot, events, budget):
        """Buy orders for quiet-alive coins in this batch. Coins are
        considered in the order their events arrive; a coin is bought at
        most once. `budget` is how many more orders this batch's reply
        may carry."""
        orders = []
        free = self.free_cash_lamports()
        slots_open = self.max_open_positions - self.committed_positions()
        if slots_open <= 0 or budget <= 0:
            return orders
        # Every mint that traded in this batch is a candidate; the cap and
        # the quiet-alive checks decide when a coin is buyable, so no
        # separate watch list is kept.
        seen = []
        for event in events or []:
            mint = event["mint"]
            if mint not in seen:
                seen.append(mint)
        for mint in seen:
            if len(orders) >= budget:
                break
            if slots_open <= 0:
                break
            if mint in self.decided or mint in self.completed:
                continue
            if mint in self.positions:
                continue
            cap = self.size_cap_lamports(mint)
            if cap < MIN_BUY_LAMPORTS:
                continue
            size = STAKE_LAMPORTS if STAKE_LAMPORTS < cap else cap
            if free < size + self.tx_fee_lamports:
                continue
            if not self.is_quiet_alive(mint, slot):
                continue
            curve = self.curves.get(mint)
            if curve is None or curve[1] == 0:
                continue
            self.buy_counter += 1
            client_id = "b%d" % self.buy_counter
            self.pending_buys[client_id] = (mint, size)
            self.decided.add(mint)
            self.entries[mint] = (curve[0], curve[1], slot)
            orders.append(
                {
                    "side": "buy",
                    "client_id": client_id,
                    "mint": mint,
                    "quote_lamports": str(size),
                    "max_slippage_bps": MAX_SLIPPAGE_BPS,
                }
            )
            free -= size + self.tx_fee_lamports
            slots_open -= 1
        return orders

    # -- main loop ---------------------------------------------------------
    def on_batch(self, batch):
        slot = int(batch["slot"])
        self.apply_fills(batch.get("fills"))
        if "account" in batch:
            self.apply_account(batch["account"], slot)
        self.apply_marks(batch.get("marks"))
        events = batch.get("events") or []
        self.apply_events(events)
        orders = []
        orders.extend(self.exit_orders(slot))
        # Sells sent this batch free no position slots until they settle, so
        # entries only use slots that are really open. The reply as a whole
        # must stay within the per-batch order cap.
        budget = self.max_orders_per_batch - len(orders)
        orders.extend(self.entry_orders(slot, events, budget))
        return {"type": "reply", "seq": batch["seq"], "orders": orders}


def main():
    bot = QuietAliveBot()
    out = sys.stdout
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        try:
            msg = json.loads(line)
        except ValueError:
            continue
        kind = msg.get("type")
        if kind == "hello":
            bot.on_hello(msg)
        elif kind == "batch":
            reply = bot.on_batch(msg)
            out.write(json.dumps(reply, separators=(",", ":")) + "\n")
            out.flush()
        elif kind == "done":
            break


if __name__ == "__main__":
    main()
