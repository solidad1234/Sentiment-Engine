"""
ingestion/ohlcv.py
──────────────────
Pulls historical 4h OHLCV bars from Binance via ccxt.
Writes to ohlcv_raw, then upserts into research_panel.

Usage:
    python -m sentiment_engine.ingestion.ohlcv --backfill
    python -m sentiment_engine.ingestion.ohlcv --incremental
"""

import time
import logging
import argparse
from datetime import datetime, timezone
from pathlib import Path

log = logging.getLogger(__name__)


# ── Helpers ────────────────────────────────────────────────────────────────

def epoch_now() -> int:
    return int(datetime.now(timezone.utc).timestamp())


def ms_to_epoch(ms: int) -> int:
    return ms // 1000


def epoch_to_dt(ts: int) -> datetime:
    return datetime.fromtimestamp(ts, tz=timezone.utc)


# ── Core Fetcher ───────────────────────────────────────────────────────────

class OHLCVIngester:
    """
    Fetches OHLCV bars from a ccxt exchange and persists them to DuckDB.

    Parameters
    ----------
    con         : duckdb connection
    exchange_id : ccxt exchange id (default 'binance')
    interval    : bar interval string (default '4h')
    """

    BARS_PER_REQUEST = 1000          # Binance max per call
    RATE_LIMIT_SLEEP = 0.3           # seconds between requests

    def __init__(self, con, exchange_id: str = "binance", interval: str = "4h"):
        try:
            import ccxt
        except ImportError:
            raise ImportError("Install ccxt: pip install ccxt")

        self.con        = con
        self.interval   = interval
        self.exchange   = getattr(ccxt, exchange_id)({"enableRateLimit": True})
        self.exchange_id = exchange_id
        log.info(f"OHLCVIngester ready — exchange={exchange_id}, interval={interval}")

    # ── Public API ─────────────────────────────────────────────────────────

    def backfill(self, asset: str, lookback_days: int = 365) -> int:
        """
        Fetch full history for `asset` going back `lookback_days`.
        Returns number of bars inserted.
        """
        symbol = f"{asset}/USDT"
        since_ms = int((time.time() - lookback_days * 86400) * 1000)

        log.info(f"[{asset}] Backfill from {epoch_to_dt(since_ms // 1000).date()} ...")
        return self._fetch_and_store(asset, symbol, since_ms)

    def incremental(self, asset: str) -> int:
        """
        Fetch only bars newer than the latest stored bar.
        Returns number of new bars inserted.
        """
        symbol   = f"{asset}/USDT"
        last_ts  = self._get_last_ts(asset)

        if last_ts is None:
            log.warning(f"[{asset}] No existing data — falling back to 30-day backfill")
            return self.backfill(asset, lookback_days=30)

        since_ms = (last_ts + 1) * 1000  # exclusive of last bar
        log.info(f"[{asset}] Incremental from {epoch_to_dt(last_ts).isoformat()}")
        return self._fetch_and_store(asset, symbol, since_ms)

    # ── Internals ──────────────────────────────────────────────────────────

    def _fetch_and_store(self, asset: str, symbol: str, since_ms: int) -> int:
        """Paginated fetch loop → parse → insert."""
        all_bars = []
        cursor   = since_ms

        while True:
            try:
                raw = self.exchange.fetch_ohlcv(
                    symbol,
                    timeframe=self.interval,
                    since=cursor,
                    limit=self.BARS_PER_REQUEST,
                )
            except Exception as e:
                log.error(f"[{asset}] ccxt error: {e}")
                break

            if not raw:
                break

            all_bars.extend(raw)
            last_open_ms = raw[-1][0]
            cursor = last_open_ms + 1  # advance past last fetched bar

            log.debug(f"[{asset}] fetched {len(raw)} bars, last={epoch_to_dt(last_open_ms // 1000)}")

            if len(raw) < self.BARS_PER_REQUEST:
                break  # reached the end

            time.sleep(self.RATE_LIMIT_SLEEP)

        if not all_bars:
            log.warning(f"[{asset}] No bars returned")
            return 0

        return self._upsert_bars(asset, all_bars)

    def _upsert_bars(self, asset: str, raw_bars: list) -> int:
        """
        Parse ccxt OHLCV list and upsert into ohlcv_raw.
        ccxt format: [timestamp_ms, open, high, low, close, volume]
        """
        now = epoch_now()
        interval_seconds = self._interval_to_seconds(self.interval)

        rows = []
        for bar in raw_bars:
            ts_open_ms, o, h, l, c, vol = bar[:6]
            ts_open  = ms_to_epoch(ts_open_ms)
            ts_close = ts_open + interval_seconds - 1
            volume_usd = c * vol  # approximation: close price × base volume

            rows.append((
                asset, self.exchange_id, self.interval,
                ts_open, ts_close,
                o, h, l, c, vol, volume_usd,
                None,       # trade_count — not always available
                now,
            ))

        # DuckDB upsert via INSERT OR REPLACE
        self.con.executemany("""
            INSERT OR REPLACE INTO ohlcv_raw
                (asset, exchange, interval, ts_open, ts_close,
                 open, high, low, close, volume, volume_usd, trade_count, ingested_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, rows)
        self.con.commit()

        log.info(f"[{asset}] Upserted {len(rows)} bars into ohlcv_raw")
        return len(rows)

    def _get_last_ts(self, asset: str):
        """Return the ts_open of the most recent stored bar, or None."""
        result = self.con.execute("""
            SELECT MAX(ts_open) FROM ohlcv_raw
            WHERE asset = ? AND exchange = ? AND interval = ?
        """, [asset, self.exchange_id, self.interval]).fetchone()
        return result[0] if result and result[0] is not None else None

    @staticmethod
    def _interval_to_seconds(interval: str) -> int:
        mapping = {"1m": 60, "5m": 300, "15m": 900, "1h": 3600, "4h": 14400, "1d": 86400}
        if interval not in mapping:
            raise ValueError(f"Unknown interval: {interval}")
        return mapping[interval]


# ── Panel Sync ────────────────────────────────────────────────────────────

def sync_ohlcv_to_panel(con, asset: str):
    """
    Upsert price columns from ohlcv_raw into research_panel.
    Computes basic price features inline (momentum, log volume, bar range).
    Forward returns are left NULL — populated after bars close.
    """
    now = epoch_now()
    con.execute(f"""
        INSERT OR REPLACE INTO research_panel (
            asset, ts_open,
            open, high, low, close, volume, volume_usd,
            log_volume, bar_range,
            price_momentum_1, price_momentum_6,
            has_price, has_sentiment, updated_at
        )
        SELECT
            o.asset,
            o.ts_open,
            o.open, o.high, o.low, o.close, o.volume, o.volume_usd,
            LN(NULLIF(o.volume_usd, 0))                                     AS log_volume,
            (o.high - o.low) / NULLIF(o.open, 0)                           AS bar_range,
            -- 1-bar momentum: log(close / prev_close)
            LN(o.close / NULLIF(
                LAG(o.close, 1) OVER (ORDER BY o.ts_open), 0))             AS price_momentum_1,
            -- 6-bar momentum: log(close / close 6 bars ago)
            LN(o.close / NULLIF(
                LAG(o.close, 6) OVER (ORDER BY o.ts_open), 0))             AS price_momentum_6,
            TRUE                                                            AS has_price,
            COALESCE(p.has_sentiment, FALSE)                                AS has_sentiment,
            {now}                                                           AS updated_at
        FROM ohlcv_raw o
        LEFT JOIN research_panel p ON p.asset = o.asset AND p.ts_open = o.ts_open
        WHERE o.asset = '{asset}'
          AND o.exchange = 'binance'
          AND o.interval = '4h'
    """)
    con.commit()
    log.info(f"[{asset}] Panel price sync complete")


# ── Entry Point ────────────────────────────────────────────────────────────

def run(mode: str = "backfill", assets=None, lookback_days: int = 365):
    import sys
    sys.path.insert(0, str(Path(__file__).parents[2]))

    from sentiment_engine.config.settings import (
        DB_PATH, ASSET_UNIVERSE, OHLCV_INTERVAL, OHLCV_EXCHANGE, OHLCV_LOOKBACK_DAYS
    )
    from sentiment_engine.storage.schema import get_connection

    target_assets = assets or ASSET_UNIVERSE
    con = get_connection(DB_PATH)
    ingester = OHLCVIngester(con, exchange_id=OHLCV_EXCHANGE, interval=OHLCV_INTERVAL)

    total_bars = 0
    for asset in target_assets:
        try:
            if mode == "backfill":
                n = ingester.backfill(asset, lookback_days=OHLCV_LOOKBACK_DAYS)
            else:
                n = ingester.incremental(asset)
            sync_ohlcv_to_panel(con, asset)
            total_bars += n
        except Exception as e:
            log.error(f"[{asset}] Failed: {e}", exc_info=True)

    log.info(f"Done. Total bars ingested: {total_bars}")
    con.close()


if __name__ == "__main__":
    import logging
    logging.basicConfig(level="INFO", format="%(asctime)s | %(levelname)s | %(message)s")

    parser = argparse.ArgumentParser(description="OHLCV Ingestion")
    parser.add_argument("--backfill",     action="store_true")
    parser.add_argument("--incremental",  action="store_true")
    parser.add_argument("--assets",       nargs="+", help="Override asset list")
    args = parser.parse_args()

    mode = "incremental" if args.incremental else "backfill"
    run(mode=mode, assets=args.assets)
