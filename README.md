# Polymarket Anomalous Trade Detector

A real-time trade monitoring system for [Polymarket](https://polymarket.com) that detects
statistically anomalous betting activity and posts alerts to Bluesky.

The detector streams every trade on active Polymarket markets via WebSocket, computes
a rolling z-score per outcome token, and flags bets that are unusually large relative to
that token's recent trading history — a pattern sometimes associated with informed or
insider trading.

---

## How It Works

1. **Market Discovery** — On startup, the script fetches all active Polymarket markets
   above a minimum volume threshold from the Gamma API.
2. **WebSocket Streaming** — Tokens are distributed across up to 50 parallel WebSocket
   connections (up to 250 tokens each), subscribing to live trade events.
3. **Per-Outcome Statistics** — Each outcome token maintains its own rolling window of
   trade sizes. The window is dual-gated: it covers the last 24 hours *or* the last 50
   trades, whichever is larger.
4. **Z-Score Evaluation** — When a new trade arrives, its USD size is compared against
   the rolling mean and standard deviation for that specific outcome token. If the
   z-score exceeds the configured threshold and other filters pass, an alert is raised.
5. **Enrichment** — The alert is enriched with the trader's wallet address (matched via
   the Data API) and, optionally, the account creation date (via Polygonscan).
6. **Bluesky Post** — The enriched alert is posted to Bluesky.

Market metadata is refreshed every 5 minutes to add newly listed markets and
remove closed ones without restarting.

---

## Alert Criteria

A trade triggers an alert only when **all** of the following conditions are met:

| Criterion | Default Value |
|---|---|
| Minimum trades in window before alerting | 30 |
| Minimum trade size | $10,000 |
| Maximum outcome price (filters near-certain events) | 0.75 |
| Z-score threshold | 3σ |

All thresholds are configurable in the `CONFIG` dictionary at the top of the script.

---

## Requirements

- Python 3.10 or newer
- A [Bluesky](https://bsky.app) account and app password
- A [Polygonscan](https://polygonscan.com/apis) API key (optional, for wallet creation dates)

### Python Dependencies

```bash
pip install requests atproto websockets
