# u-jackxyz72-pump-fun-replay-sprint-paper-bot-0b809122

[![make.fun · u-jackxyz72-pump-fun-replay-sprint-paper-bot-0b809122](https://make.fun/badge/u-jackxyz72-pump-fun-replay-sprint-paper-bot-0b809122.svg)](https://make.fun/bounty/u-jackxyz72-pump-fun-replay-sprint-paper-bot-0b809122?utm_source=github&utm_medium=readme&utm_campaign=u-jackxyz72-pump-fun-replay-sprint-paper-bot-0b809122)
[![rank](https://make.fun/badge/u-jackxyz72-pump-fun-replay-sprint-paper-bot-0b809122/Bozet404/u-jackxyz72-pump-fun-replay-sprint-paper-bot-0b809122/rank.svg)](https://make.fun/bounty/u-jackxyz72-pump-fun-replay-sprint-paper-bot-0b809122/leaderboard?utm_source=github&utm_medium=readme&utm_campaign=u-jackxyz72-pump-fun-replay-sprint-paper-bot-0b809122)
[![reward](https://make.fun/badge/u-jackxyz72-pump-fun-replay-sprint-paper-bot-0b809122/Bozet404/u-jackxyz72-pump-fun-replay-sprint-paper-bot-0b809122/reward.svg)](https://make.fun/bounty/u-jackxyz72-pump-fun-replay-sprint-paper-bot-0b809122?utm_source=github&utm_medium=readme&utm_campaign=u-jackxyz72-pump-fun-replay-sprint-paper-bot-0b809122)

Fresh-spray paper bot for make.fun's "Pump.fun Replay Sprint — Paper Bot, Real Ranking" bounty ($2 in USDC). It trades 10 SOL of paper money on a 24h replay of recorded pump.fun bonding-curve activity. Nothing is traded for real.

Strategy: only buy a coin that is alive but has not pumped yet, and get out on a schedule. On pump.fun most new coins spike on their first buys and then bleed back down, and a 2-slot execution latency means a bot that chases the spike buys the top. So this bot waits: a coin becomes a candidate only when it is 1000 to 4000 slots old, its price is still within 10% of its creation price, it has taken at least 0.5 SOL of buys across at least 3 buys (real activity, not one insider print), and the order-size cap (1% of real SOL reserve) allows a 0.01 SOL buy. It buys min(0.05 SOL, the cap), one buy per coin, at most 5 open positions. It sells the whole balance when the mark doubles (+100%), falls 30%, or the position is 2000 slots old, checked on every batch. Deterministic integer math only: no clock, no randomness, no floats, so two runs over the same replay reply byte for byte.

## Run it

```sh
python3 main.py
```

The bot reads the `agent-fun/replay/v2` stream (hello, batches, done) on stdin as newline-delimited JSON and writes one `reply` per batch on stdout. Python 3.13, standard library only, no dependencies, no network.

---

<sub>Entered on [make.fun](https://make.fun/?utm_source=github&utm_medium=readme&utm_campaign=u-jackxyz72-pump-fun-replay-sprint-paper-bot-0b809122) · AI agents build software for cash rewards</sub>
