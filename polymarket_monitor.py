"""
Polymarket Anomalous Trade Detector — v5.3 (WebSocket, Production)
──────────────────────────────────────────────────────────────────
Streams every Polymarket trade in real time via WebSocket.
Uses a dual-window (time + count) rolling z-score per outcome token.
Anomalous bets are enriched with trader wallet address and
account creation date, then posted to Bluesky.

Prerequisites:
    pip install requests atproto websockets

Environment variables:
    BSKY_HANDLE          – Bluesky handle (e.g. user.bsky.social)
    BSKY_PASSWORD        – Bluesky app password
    POLYGONSCAN_API_KEY  – (optional) for account creation-date lookups

Changes from v5.2:
    • websockets v14+ compatibility (_ws_is_open helper).
    • O(1) dedup lookup via dict instead of O(n) deque scan.
    • Stats keyed per asset_id (per-outcome) for accurate z-scores.
    • Stats cleanup aligned to asset_id keys (no more memory leak).
    • Dynamically spawned WS tasks are tracked for clean shutdown.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from statistics import mean, stdev
from typing import Optional

import requests
import websockets


# ────────────────────────────────────────────────────────────
# CONFIGURATION
# ────────────────────────────────────────────────────────────

CONFIG = {
    # ── API endpoints ───────────────────────────────────────
    "GAMMA_API":       "https://gamma-api.polymarket.com",
    "DATA_API":        "https://data-api.polymarket.com",
    "WS_URL":          "wss://ws-subscriptions-clob.polymarket.com/ws/market",
    "POLYGONSCAN_API": "https://api.polygonscan.com/api",

    # ── WebSocket ───────────────────────────────────────────
    "MAX_TOKENS_PER_CONNECTION": 250,
    "MAX_WS_CONNECTIONS":        50,
    "WS_CONNECT_STAGGER_DELAY":  1.0,
    "MARKET_REFRESH_INTERVAL":   300,
    "WS_HEARTBEAT_INTERVAL":     10,
    "WS_RECONNECT_DELAY":        5,
    "MIN_MARKET_VOLUME":         50_000,

    # ── Analysis windows ────────────────────────────────────
    "TIME_WINDOW_HOURS":  24,
    "MIN_COUNT_FLOOR":    50,
    "MAX_WINDOW_SIZE":    24_000,

    # ── Alert thresholds ────────────────────────────────────
    "MIN_WINDOW_BEFORE_ALERT": 30,
    "Z_SCORE_THRESHOLD":       3,
    "MIN_BET_USD":             10_000,
    "FLOW_IMBALANCE_THRESHOLD": 0.70,
    "MAX_PRICE_TO_FLAG":       0.75,

    # ── Bluesky ─────────────────────────────────────────────
    "BSKY_HANDLE":   os.getenv("BSKY_HANDLE", ""),
    "BSKY_PASSWORD": os.getenv("BSKY_PASSWORD", ""),
    "POST_TO_BLUESKY": True,

    # ── Polygonscan (optional) ──────────────────────────────
    "POLYGONSCAN_API_KEY": os.getenv("POLYGONSCAN_API_KEY", ""),

    # ── Internal ────────────────────────────────────────────
    "SEEN_CACHE_SIZE":      10_000,
    "ALERT_QUEUE_SIZE":     100,
    "ENRICHMENT_TIMEOUT":   15,
    "BACKFILL_SAMPLE_SIZE": 20,
}


# ────────────────────────────────────────────────────────────
# LOGGING
# ────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("polymarket-monitor")


# ────────────────────────────────────────────────────────────
# DATA STRUCTURES
# ────────────────────────────────────────────────────────────

@dataclass
class TradeRecord:
    size_usd:  float
    timestamp: datetime
    is_buy:    bool


@dataclass
class MarketStats:
    records: deque = field(
        default_factory=lambda: deque(maxlen=CONFIG["MAX_WINDOW_SIZE"])
    )
    _cache_key: float = field(default=0.0, repr=False)
    _cache_val: list  = field(default_factory=list, repr=False)

    def add(self, rec: TradeRecord) -> None:
        self.records.append(rec)
        self._cache_key = 0.0

    def _window(self) -> list[TradeRecord]:
        now = time.time()
        if now - self._cache_key < 0.001 and self._cache_val:
            return self._cache_val

        if not self.records:
            self._cache_val = []
            self._cache_key = now
            return self._cache_val

        cutoff   = datetime.now(timezone.utc) - timedelta(hours=CONFIG["TIME_WINDOW_HOURS"])
        by_time  = [r for r in self.records if r.timestamp >= cutoff]
        by_count = list(self.records)[-CONFIG["MIN_COUNT_FLOOR"]:]
        self._cache_val = by_time if len(by_time) >= len(by_count) else by_count
        self._cache_key = now
        return self._cache_val

    @property
    def count(self) -> int:
        return len(self._window())

    @property
    def rolling_mean(self) -> float:
        w = self._window()
        return mean(r.size_usd for r in w) if w else 0.0

    @property
    def rolling_std(self) -> float:
        w = self._window()
        s = [r.size_usd for r in w]
        return stdev(s) if len(s) >= 2 else 0.0

    def z_score(self, size_usd: float) -> Optional[float]:
        sd = self.rolling_std
        return None if sd == 0 else (size_usd - self.rolling_mean) / sd

    def flow_imbalance(self) -> Optional[float]:
        w = self._window()
        if not w:
            return None
        buy   = sum(r.size_usd for r in w if r.is_buy)
        total = sum(r.size_usd for r in w)
        return buy / total if total else None


@dataclass
class TradeAlert:
    trade_id:           str
    market_id:          str
    asset_id:           str
    market_question:    str
    outcome:            str
    size_usd:           float
    price:              float
    side:               str
    z_score:            float
    rolling_mean:       float
    rolling_std:        float
    window_count:       int
    flow_imbalance:     Optional[float]
    display_time:       str
    trade_timestamp_ms: int               = 0
    trader_address:     Optional[str]     = None
    account_created:    Optional[str]     = None


# ────────────────────────────────────────────────────────────
# MARKET FETCHER  (Gamma API)
# ────────────────────────────────────────────────────────────

class MarketFetcher:
    def __init__(self) -> None:
        self.session = requests.Session()
        self.session.headers["User-Agent"] = "polymarket-monitor/5.3"
        self.token_metadata: dict[str, dict] = {}

    def fetch_all_active_tokens(self) -> list[str]:
        seen_tokens: set[str] = set()
        new_meta:    dict[str, dict] = {}
        skipped_low_vol = 0
        offset       = 0
        limit        = 100
        max_pages    = 500
        empty_streak = 0
        min_vol      = CONFIG["MIN_MARKET_VOLUME"]

        for page in range(max_pages):
            try:
                resp = self.session.get(
                    f"{CONFIG['GAMMA_API']}/markets",
                    params={
                        "active": "true",
                        "closed": "false",
                        "limit":  limit,
                        "offset": offset,
                    },
                    timeout=30,
                )
                resp.raise_for_status()
                raw = resp.json()

                if isinstance(raw, dict):
                    markets = raw.get("data", raw.get("markets", []))
                elif isinstance(raw, list):
                    markets = raw
                else:
                    break

                if not markets:
                    break

                new_on_page = 0

                for mkt in markets:
                    cid = mkt.get("conditionId", "") or mkt.get("condition_id", "")
                    if not cid:
                        continue

                    try:
                        vol = float(
                            mkt.get("volumeNum", 0)
                            or mkt.get("volume", 0)
                            or mkt.get("volume_num", 0)
                            or 0
                        )
                    except (ValueError, TypeError):
                        vol = 0.0

                    try:
                        liq = float(
                            mkt.get("liquidityNum", 0)
                            or mkt.get("liquidity", 0)
                            or mkt.get("liquidity_num", 0)
                            or 0
                        )
                    except (ValueError, TypeError):
                        liq = 0.0

                    if vol < min_vol and liq < min_vol:
                        skipped_low_vol += 1
                        continue

                    ids = mkt.get("clobTokenIds", mkt.get("clob_token_ids", []))
                    if isinstance(ids, str):
                        try:
                            ids = json.loads(ids)
                        except (json.JSONDecodeError, TypeError):
                            continue
                    if not isinstance(ids, list) or not ids:
                        continue

                    q = mkt.get("question", "Unknown")

                    raw_oc = mkt.get("outcomes", "")
                    try:
                        ol = json.loads(raw_oc) if isinstance(raw_oc, str) else raw_oc
                    except (json.JSONDecodeError, TypeError):
                        ol = None
                    if not isinstance(ol, list):
                        ol = ["Yes", "No"]

                    for i, tid in enumerate(ids):
                        if tid and tid not in seen_tokens:
                            seen_tokens.add(tid)
                            new_on_page += 1
                            oc = ol[i] if i < len(ol) else f"Outcome {i}"
                            new_meta[tid] = {
                                "question":     q,
                                "condition_id": cid,
                                "outcome":      oc,
                            }

                if new_on_page == 0:
                    empty_streak += 1
                    if empty_streak >= 3:
                        break
                else:
                    empty_streak = 0

                if len(markets) < limit:
                    break

                offset += limit

            except requests.RequestException as exc:
                log.error("Market fetch error at page %d (offset %d): %s", page, offset, exc)
                break

        self.token_metadata.update(new_meta)
        tokens = list(seen_tokens)
        n_mkts = len({m["condition_id"] for m in new_meta.values()})
        log.info(
            "Fetched %d tokens across %d markets "
            "(%d skipped below $%s vol/liq, %d pages scanned)",
            len(tokens), n_mkts, skipped_low_vol, f"{min_vol:,.0f}", page + 1,
        )
        return tokens

    def get_metadata(self, token_id: str) -> dict:
        return self.token_metadata.get(token_id, {
            "question":     "Unknown Market",
            "condition_id": token_id,
            "outcome":      "Unknown",
        })


# ────────────────────────────────────────────────────────────
# TRADE ENRICHER  (Data API + Polygonscan)
# ────────────────────────────────────────────────────────────

class TradeEnricher:
    def __init__(self) -> None:
        self._session = requests.Session()
        self._session.headers["User-Agent"] = "polymarket-monitor/5.3"
        self._creation_cache: dict[str, Optional[str]] = {}

    def enrich(self, alert: TradeAlert) -> TradeAlert:
        wallet = self._find_wallet(alert)
        if wallet:
            alert.trader_address  = wallet
            alert.account_created = self._creation_date(wallet)
        return alert

    def _find_wallet(self, alert: TradeAlert) -> Optional[str]:
        try:
            r = self._session.get(
                f"{CONFIG['DATA_API']}/trades",
                params={"asset_id": alert.asset_id, "limit": 50},
                timeout=CONFIG["ENRICHMENT_TIMEOUT"],
            )
            r.raise_for_status()
            payload = r.json()
            trades = (
                payload if isinstance(payload, list)
                else payload.get("data", payload.get("trades", []))
            )

            alert_dt = datetime.fromtimestamp(
                alert.trade_timestamp_ms / 1000, tz=timezone.utc
            )
            usd_tol      = max(1.0, alert.size_usd * 0.01)
            best_wallet  = None
            best_gap     = float("inf")

            for t in trades:
                try:
                    sz = float(t.get("size", 0))
                    px = float(t.get("price", 0))
                    sd = t.get("side", "").upper()
                except (ValueError, TypeError):
                    continue

                if sd != alert.side or abs(sz * px - alert.size_usd) >= usd_tol:
                    continue

                wallet = (
                    t.get("proxyWallet")
                    or t.get("maker_address")
                    or t.get("taker_address")
                    or t.get("owner")
                )
                if not wallet:
                    continue

                raw_ts = t.get("timestamp", t.get("match_time", ""))
                try:
                    if isinstance(raw_ts, (int, float)):
                        tdt = datetime.fromtimestamp(
                            raw_ts / 1000 if raw_ts > 1e12 else raw_ts,
                            tz=timezone.utc,
                        )
                    else:
                        tdt = datetime.fromisoformat(
                            str(raw_ts).replace("Z", "+00:00")
                        )
                    gap = abs((tdt - alert_dt).total_seconds())
                except (ValueError, TypeError):
                    gap = 999

                if gap < best_gap:
                    best_gap    = gap
                    best_wallet = wallet

            return best_wallet if best_gap < 60 else None

        except Exception as exc:
            log.debug("Wallet lookup failed: %s", exc)
            return None

    def _creation_date(self, wallet: str) -> Optional[str]:
        if wallet in self._creation_cache:
            return self._creation_cache[wallet]

        for action in ("txlist", "txlistinternal"):
            try:
                params: dict = {
                    "module": "account", "action": action,
                    "address": wallet,
                    "startblock": 0, "endblock": 99999999,
                    "page": 1, "offset": 1, "sort": "asc",
                }
                key = CONFIG.get("POLYGONSCAN_API_KEY", "")
                if key:
                    params["apikey"] = key

                r = self._session.get(
                    CONFIG["POLYGONSCAN_API"], params=params,
                    timeout=CONFIG["ENRICHMENT_TIMEOUT"],
                )
                r.raise_for_status()
                body = r.json()

                if body.get("status") == "1" and body.get("result"):
                    ts = int(body["result"][0].get("timeStamp", 0))
                    if ts > 0:
                        created = datetime.fromtimestamp(
                            ts, tz=timezone.utc
                        ).strftime("%Y-%m-%d")
                        self._creation_cache[wallet] = created
                        return created

            except Exception as exc:
                log.debug("Creation-date lookup (%s) failed: %s", action, exc)

        self._creation_cache[wallet] = None
        return None


# ────────────────────────────────────────────────────────────
# BLUESKY POSTER
# ────────────────────────────────────────────────────────────

class BlueskyPoster:
    def __init__(self) -> None:
        self._client = None
        self._ok     = False

    def _login(self) -> None:
        if not self._ok:
            from atproto import Client
            self._client = Client()
            self._client.login(CONFIG["BSKY_HANDLE"], CONFIG["BSKY_PASSWORD"])
            self._ok = True
            log.info("Bluesky: logged in as %s", CONFIG["BSKY_HANDLE"])

    def post(self, alert: TradeAlert) -> None:
        txt = self._fmt(alert)
        if not CONFIG["POST_TO_BLUESKY"]:
            log.info("[DRY-RUN POST]\n%s", txt)
            return
        try:
            self._login()
            self._client.send_post(text=txt[:300])
            log.info("Bluesky post sent (%d chars)", len(txt))
        except Exception as exc:
            log.error("Bluesky post failed: %s", exc)
            self._ok = False

    @staticmethod
    def _fmt(a: TradeAlert) -> str:
        flow = ""
        if a.flow_imbalance is not None:
            bp  = int(a.flow_imbalance * 100)
            sp  = 100 - bp
            thr = int(CONFIG["FLOW_IMBALANCE_THRESHOLD"] * 100)
            if bp >= thr:
                flow = f"\n⚠️ {bp}% buy flow"
            elif sp >= thr:
                flow = f"\n⚠️ {sp}% sell flow"

        trader = ""
        if a.trader_address:
            short = f"{a.trader_address[:6]}…{a.trader_address[-4:]}"
            if a.account_created:
                trader = f"\n👤 {short} (since {a.account_created})"
            else:
                trader = f"\n👤 {short}"

        q = a.market_question.replace("\n", " ").strip()
        if len(q) > 52:
            q = q[:51] + "…"

        return (
            f"🚨 Polymarket Anomaly\n"
            f"📋 {q}\n"
            f"🎯 {a.outcome} — {a.side}\n"
            f"💰 ${a.size_usd:,.0f} @ {a.price:.3f}\n"
            f"📊 z={a.z_score:.1f}σ | μ=${a.rolling_mean:,.0f} | n={a.window_count}"
            f"{flow}{trader}\n"
            f"🕐 {a.display_time}"
        )


# ────────────────────────────────────────────────────────────
# WEBSOCKET MONITOR  (core orchestrator)
# ────────────────────────────────────────────────────────────

class WebSocketMonitor:
    def __init__(self) -> None:
        self.fetcher  = MarketFetcher()
        self.enricher = TradeEnricher()
        self.poster   = BlueskyPoster()
        self.stats:    dict[str, MarketStats] = defaultdict(MarketStats)
        self.seen:     dict[str, None] = {}                                # FIX 1: O(1) dict

        self.current_tokens: set[str]                                        = set()
        self._conn_tokens:   dict[int, set[str]]                            = {}
        self._active_ws:     dict[int, object]                              = {}
        self._ws_lock                                                        = asyncio.Lock()
        self._alert_q:       asyncio.Queue[TradeAlert]                      = asyncio.Queue(
            maxsize=CONFIG["ALERT_QUEUE_SIZE"]
        )
        self._tasks:         list[asyncio.Task] = []                        # FIX 3: track tasks

        self._backfill_session = requests.Session()
        self._backfill_session.headers["User-Agent"] = "polymarket-monitor/5.3"

    # ─────────── websockets version compatibility ────────────────────

    @staticmethod
    def _ws_is_open(ws) -> bool:                                            # FIX 0: v14 compat
        """Return True if the WebSocket connection is still open.

        Compatible with both the legacy WebSocketClientProtocol
        (.closed attribute) and the newer ClientConnection
        (.close_code attribute) introduced in websockets v14.
        """
        if hasattr(ws, "closed"):
            return not ws.closed
        if hasattr(ws, "close_code"):
            return ws.close_code is None
        return False

    # ─────────── event processing ────────────────────────────────────

    def _on_event(self, evt: dict) -> None:
        if evt.get("event_type") != "last_trade_price":
            return
        try:
            aid   = evt.get("asset_id", "")
            px    = float(evt.get("price", 0))
            sz    = float(evt.get("size", 0))
            usd   = sz * px
            side  = evt.get("side", "UNKNOWN").upper()
            ts_ms = int(evt.get("timestamp", 0))

            uid = f"{ts_ms}-{aid}-{sz}-{px}-{side}"

            # ── FIX 1: O(1) dedup via dict ──
            if uid in self.seen:
                return
            self.seen[uid] = None
            if len(self.seen) > CONFIG["SEEN_CACHE_SIZE"]:
                self.seen.pop(next(iter(self.seen)))
            # ── END FIX 1 ──

            if usd <= 0:
                return

            meta = self.fetcher.get_metadata(aid)
            key  = aid                                                      # FIX 4: per-outcome
            st   = self.stats[key]
            dt   = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)
            rec  = TradeRecord(size_usd=usd, timestamp=dt, is_buy=(side == "BUY"))

            alert = self._eval(uid, aid, usd, key, meta["outcome"],
                               px, side, st, meta["question"], ts_ms)
            st.add(rec)

            if alert:
                try:
                    self._alert_q.put_nowait(alert)
                except asyncio.QueueFull:
                    log.warning("Alert queue full — dropping alert")

        except (KeyError, ValueError, TypeError) as exc:
            log.debug("Event parse error: %s", exc)

    def _eval(self, tid, aid, usd, mid, outcome, px, side, st, question, ts_ms
              ) -> Optional[TradeAlert]:
        if st.count < CONFIG["MIN_WINDOW_BEFORE_ALERT"]:
            return None
        if usd < CONFIG["MIN_BET_USD"]:
            return None
        if px > CONFIG["MAX_PRICE_TO_FLAG"]:
            return None
        z = st.z_score(usd)
        if z is None or z < CONFIG["Z_SCORE_THRESHOLD"]:
            return None
        return TradeAlert(
            trade_id=tid, market_id=mid, asset_id=aid,
            market_question=question, outcome=outcome,
            size_usd=usd, price=px, side=side, z_score=z,
            rolling_mean=st.rolling_mean, rolling_std=st.rolling_std,
            window_count=st.count, flow_imbalance=st.flow_imbalance(),
            display_time=datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
            trade_timestamp_ms=ts_ms,
        )

    # ─────────── alert consumer ──────────────────────────────────────

    async def _consume_alerts(self) -> None:
        loop = asyncio.get_running_loop()
        while True:
            alert = await self._alert_q.get()
            try:
                alert = await loop.run_in_executor(
                    None, self.enricher.enrich, alert
                )
                extra = ""
                if alert.trader_address:
                    s = f"{alert.trader_address[:6]}…{alert.trader_address[-4:]}"
                    extra = f" | {s}"
                    if alert.account_created:
                        extra += f" (since {alert.account_created})"

                log.warning(
                    "ANOMALY | %s | %s %s | $%s | z=%.2fσ%s",
                    alert.market_question[:55], alert.outcome, alert.side,
                    f"{alert.size_usd:,.0f}", alert.z_score, extra,
                )
                await loop.run_in_executor(None, self.poster.post, alert)

            except Exception as exc:
                log.error("Alert processing error: %s", exc)
            finally:
                self._alert_q.task_done()

    # ─────────── backfill after reconnection ─────────────────────────

    def _fetch_backfill_events(self, tokens: list[str], since: float
                               ) -> list[dict]:
        since_dt = datetime.fromtimestamp(since, tz=timezone.utc)
        events: list[dict] = []
        sample = tokens[:CONFIG["BACKFILL_SAMPLE_SIZE"]]

        for tid in sample:
            try:
                r = self._backfill_session.get(
                    f"{CONFIG['DATA_API']}/trades",
                    params={"asset_id": tid, "limit": 100},
                    timeout=15,
                )
                if r.status_code != 200:
                    continue
                payload = r.json()
                trades = (
                    payload if isinstance(payload, list)
                    else payload.get("data", payload.get("trades", []))
                )
                for t in trades:
                    raw_ts = t.get("timestamp", t.get("match_time", ""))
                    try:
                        if isinstance(raw_ts, (int, float)):
                            tdt = datetime.fromtimestamp(
                                raw_ts / 1000 if raw_ts > 1e12 else raw_ts,
                                tz=timezone.utc,
                            )
                        else:
                            tdt = datetime.fromisoformat(
                                str(raw_ts).replace("Z", "+00:00")
                            )
                    except (ValueError, TypeError):
                        continue
                    if tdt < since_dt:
                        continue
                    events.append({
                        "event_type": "last_trade_price",
                        "asset_id":   tid,
                        "price":      str(t.get("price", 0)),
                        "size":       str(t.get("size", 0)),
                        "side":       t.get("side", "UNKNOWN"),
                        "timestamp":  str(int(tdt.timestamp() * 1000)),
                        "market":     t.get("conditionId", t.get("market", "")),
                    })
            except requests.RequestException:
                continue

        return events

    # ─────────── single WebSocket connection loop ────────────────────

    async def _ws_loop(self, idx: int, initial_tokens: list[str]) -> None:
        delay = idx * CONFIG["WS_CONNECT_STAGGER_DELAY"]
        if delay > 0:
            log.debug("WS-%d: waiting %.1fs before connecting", idx, delay)
            await asyncio.sleep(delay)

        last_ts: Optional[float] = None
        loop = asyncio.get_running_loop()

        while True:
            heartbeat_task: Optional[asyncio.Task] = None
            try:
                async with websockets.connect(
                    CONFIG["WS_URL"],
                    ping_interval=None,
                    ping_timeout=None,
                    close_timeout=5,
                ) as ws:
                    log.info("WS-%d connected", idx)
                    async with self._ws_lock:
                        self._active_ws[idx] = ws

                    async def _heartbeat():
                        try:
                            while True:
                                await asyncio.sleep(CONFIG["WS_HEARTBEAT_INTERVAL"])
                                await ws.send("PING")
                        except (asyncio.CancelledError,
                                websockets.ConnectionClosed):
                            pass

                    heartbeat_task = asyncio.create_task(_heartbeat())

                    if last_ts is not None:
                        gap = time.time() - last_ts
                        if gap > 5:
                            log.info("WS-%d: backfilling %.0fs gap", idx, gap)
                            batch = list(
                                self._conn_tokens.get(idx, set(initial_tokens))
                            )
                            evts = await loop.run_in_executor(
                                None, self._fetch_backfill_events, batch, last_ts
                            )
                            for e in evts:
                                self._on_event(e)
                            log.info("WS-%d: backfilled %d events", idx, len(evts))

                    batch = list(self._conn_tokens.get(idx, set(initial_tokens)))
                    await ws.send(json.dumps({
                        "assets_ids": batch,
                        "type": "market",
                    }))
                    log.info("WS-%d subscribed to %d tokens", idx, len(batch))

                    async for msg in ws:
                        last_ts = time.time()

                        if isinstance(msg, str) and msg.strip().upper() == "PONG":
                            continue

                        try:
                            data = json.loads(msg)
                            if isinstance(data, list):
                                for e in data:
                                    self._on_event(e)
                            elif isinstance(data, dict):
                                self._on_event(data)
                        except json.JSONDecodeError:
                            pass

            except websockets.ConnectionClosed as exc:
                log.warning("WS-%d closed (code=%s). Reconnecting…", idx, exc.code)
            except asyncio.CancelledError:
                log.info("WS-%d cancelled", idx)
                return
            except Exception as exc:
                log.error("WS-%d error: %s. Reconnecting…", idx, exc)
            finally:
                if heartbeat_task and not heartbeat_task.done():
                    heartbeat_task.cancel()
                async with self._ws_lock:
                    self._active_ws.pop(idx, None)

            await asyncio.sleep(CONFIG["WS_RECONNECT_DELAY"])

    # ─────────── periodic market refresh (add + remove) ──────────────

    async def _refresh_loop(self) -> None:
        loop    = asyncio.get_running_loop()
        next_id = max(self._conn_tokens.keys(), default=-1) + 1

        while True:
            await asyncio.sleep(CONFIG["MARKET_REFRESH_INTERVAL"])
            try:
                log.info("Refreshing market list…")
                new_all = set(
                    await loop.run_in_executor(
                        None, self.fetcher.fetch_all_active_tokens
                    )
                )
                added   = new_all - self.current_tokens
                removed = self.current_tokens - new_all

                # ── REMOVE closed markets ──
                if removed:
                    log.info("Removing %d tokens from closed markets", len(removed))

                    # FIX 2: clean up stats by asset_id (aligned with FIX 4)
                    for tid in removed:
                        self.stats.pop(tid, None)
                        self.fetcher.token_metadata.pop(tid, None)

                    conns_to_reconnect: list[int] = []
                    for idx, tset in list(self._conn_tokens.items()):
                        before = len(tset)
                        tset -= removed
                        after  = len(tset)

                        if before == after:
                            continue

                        log.debug(
                            "WS-%d: pruned %d closed tokens (%d remaining)",
                            idx, before - after, after,
                        )

                        if after == 0:
                            conns_to_reconnect.append(idx)
                        elif (before - after) >= before * 0.25:
                            conns_to_reconnect.append(idx)

                    for idx in conns_to_reconnect:
                        async with self._ws_lock:
                            ws = self._active_ws.get(idx)
                            if ws and self._ws_is_open(ws):                 # FIX 0
                                log.info(
                                    "WS-%d: closing to apply removals",
                                    idx,
                                )
                                await ws.close()

                        if not self._conn_tokens.get(idx):
                            self._conn_tokens.pop(idx, None)
                            log.info("WS-%d: slot freed (all tokens closed)", idx)

                # ── ADD new markets ──
                if added:
                    added_list = list(added)
                    log.info("Subscribing to %d new tokens", len(added_list))

                    cap = CONFIG["MAX_TOKENS_PER_CONNECTION"]

                    remaining = list(added_list)

                    while remaining:
                        batch = remaining[:cap]

                        target = None
                        for i, tset in self._conn_tokens.items():
                            space = cap - len(tset)
                            if space >= len(batch):
                                target = i
                                break
                            elif space > 0:
                                batch  = remaining[:space]
                                target = i
                                break

                        if target is not None:
                            self._conn_tokens[target].update(batch)
                            async with self._ws_lock:
                                ws = self._active_ws.get(target)
                                if ws and self._ws_is_open(ws):             # FIX 0
                                    try:
                                        await ws.send(json.dumps({
                                            "assets_ids": batch,
                                            "type": "market",
                                        }))
                                        log.info(
                                            "Added %d tokens to WS-%d (%d total)",
                                            len(batch), target,
                                            len(self._conn_tokens[target]),
                                        )
                                    except Exception as exc:
                                        log.debug(
                                            "Subscribe on WS-%d failed: %s",
                                            target, exc,
                                        )
                            remaining = remaining[len(batch):]

                        elif len(self._active_ws) < CONFIG["MAX_WS_CONNECTIONS"]:
                            batch = remaining[:cap]
                            self._conn_tokens[next_id] = set(batch)
                            # FIX 3: track dynamically spawned tasks
                            task = asyncio.create_task(
                                self._ws_loop(next_id, batch)
                            )
                            self._tasks.append(task)
                            log.info(
                                "Spawned WS-%d for %d new tokens",
                                next_id, len(batch),
                            )
                            next_id  += 1
                            remaining = remaining[len(batch):]

                        else:
                            log.warning(
                                "Cannot subscribe to %d remaining new tokens "
                                "— all %d connection slots are full",
                                len(remaining),
                                CONFIG["MAX_WS_CONNECTIONS"],
                            )
                            break

                # ── Summary ──
                active_conns  = len(self._conn_tokens)
                active_tokens = sum(len(t) for t in self._conn_tokens.values())
                log.info(
                    "Refresh complete: %d tokens across %d connections "
                    "(+%d added, -%d removed)",
                    active_tokens, active_conns, len(added), len(removed),
                )

                self.current_tokens = new_all

            except Exception as exc:
                log.error("Market refresh failed: %s", exc)

    # ─────────── main entry point ────────────────────────────────────

    async def run(self) -> None:
        log.info(
            "Polymarket Monitor v5.3 | z≥%sσ | min=$%s | window=%dh",
            CONFIG["Z_SCORE_THRESHOLD"],
            f"{CONFIG['MIN_BET_USD']:,}",
            CONFIG["TIME_WINDOW_HOURS"],
        )

        all_tokens = self.fetcher.fetch_all_active_tokens()
        self.current_tokens = set(all_tokens)

        if not all_tokens:
            log.error("No active tokens found. Exiting.")
            return

        cap      = CONFIG["MAX_TOKENS_PER_CONNECTION"]
        max_conn = CONFIG["MAX_WS_CONNECTIONS"]
        capacity = cap * max_conn

        if len(all_tokens) > capacity:
            log.warning(
                "Token count (%d) exceeds max capacity "
                "(%d connections × %d tokens = %d). "
                "Only monitoring first %d tokens.",
                len(all_tokens), max_conn, cap, capacity, capacity,
            )
            all_tokens = all_tokens[:capacity]
            self.current_tokens = set(all_tokens)

        batches = [all_tokens[i:i + cap] for i in range(0, len(all_tokens), cap)]
        for i, b in enumerate(batches):
            self._conn_tokens[i] = set(b)

        log.info(
            "Opening %d WebSocket connection(s) for %d tokens "
            "(stagger: %.1fs between each)",
            len(batches), len(all_tokens), CONFIG["WS_CONNECT_STAGGER_DELAY"],
        )

        # FIX 3: use self._tasks so dynamic spawns are included
        self._tasks = [asyncio.create_task(self._ws_loop(i, b))
                       for i, b in enumerate(batches)]
        self._tasks.append(asyncio.create_task(self._refresh_loop()))
        self._tasks.append(asyncio.create_task(self._consume_alerts()))

        try:
            await asyncio.gather(*self._tasks)
        except KeyboardInterrupt:
            log.info("Shutting down…")
            for t in self._tasks:
                t.cancel()


# ────────────────────────────────────────────────────────────
# ENTRY POINT
# ────────────────────────────────────────────────────────────

if __name__ == "__main__":
    monitor = WebSocketMonitor()
    asyncio.run(monitor.run())
