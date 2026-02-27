"""
ingestion/orchestrator.py
──────────────────────────
Top-level ingestion coordinator.

Runs:
  1. OHLCV backfill / incremental update
  2. Reddit scrape
  3. Twitter scrape (if credentials available)
  4. Data quality checks
  5. Research panel sync

Usage:
    python -m sentiment_engine.ingestion.orchestrator --mode backfill
    python -m sentiment_engine.ingestion.orchestrator --mode incremental
    python -m sentiment_engine.ingestion.orchestrator --mode backfill --assets BTC ETH SOL
"""

import logging
import argparse
import sys
from pathlib import Path
from datetime import datetime, timezone
from dotenv import load_dotenv; load_dotenv()

sys.path.insert(0, str(Path(__file__).parents[2]))

from sentiment_engine.config.settings import (
    DB_PATH, ASSET_UNIVERSE, ASSET_SUBREDDIT_MAP,
    ASSET_TWITTER_KEYWORDS, REDDIT_SUBREDDITS,
    OHLCV_EXCHANGE, OHLCV_INTERVAL, OHLCV_LOOKBACK_DAYS,
    MIN_POSTS_PER_WINDOW, LOG_FORMAT, LOG_LEVEL,
)
from sentiment_engine.storage.schema import init_db, get_connection
from sentiment_engine.ingestion.ohlcv import OHLCVIngester, sync_ohlcv_to_panel
from sentiment_engine.ingestion.reddit_scraper import RedditScraper
from sentiment_engine.ingestion.twitter_scraper import get_twitter_ingester

log = logging.getLogger(__name__)


# ── Data Quality Checks ────────────────────────────────────────────────────

def run_data_quality_checks(con):
    """
    Runs a set of DQ checks after ingestion.
    Writes flagged rows to data_quality_log.
    """
    now = int(datetime.now(timezone.utc).timestamp())
    checks_run = 0

    # 1. Bars with zero volume
    result = con.execute("""
        SELECT asset, ts_open, 'zero_volume' AS check_name, 'warn' AS severity,
               CONCAT('volume=', volume) AS detail
        FROM ohlcv_raw
        WHERE volume = 0 OR volume IS NULL
    """).fetchall()
    _log_dq_issues(con, result, now)
    checks_run += 1

    # 2. OHLC sanity (high < low, etc.)
    result = con.execute("""
        SELECT asset, ts_open, 'ohlc_invalid' AS check_name, 'error' AS severity,
               CONCAT('h=', high, ' l=', low) AS detail
        FROM ohlcv_raw
        WHERE high < low OR open <= 0 OR close <= 0
    """).fetchall()
    _log_dq_issues(con, result, now)
    checks_run += 1

    # 3. Duplicate bar timestamps (shouldn't happen with PRIMARY KEY, but verify)
    result = con.execute("""
        SELECT asset, ts_open, 'duplicate_bar' AS check_name, 'error' AS severity,
               CONCAT('count=', cnt) AS detail
        FROM (
            SELECT asset, ts_open, COUNT(*) AS cnt
            FROM ohlcv_raw
            GROUP BY asset, ts_open
            HAVING cnt > 1
        )
    """).fetchall()
    _log_dq_issues(con, result, now)
    checks_run += 1

    # 4. Price gaps > 3x previous close (likely data error)
    result = con.execute("""
        SELECT asset, ts_open, 'price_gap' AS check_name, 'warn' AS severity,
               CONCAT('ratio=', ROUND(close / prev_close, 2)) AS detail
        FROM (
            SELECT asset, ts_open, close,
                   LAG(close) OVER (PARTITION BY asset ORDER BY ts_open) AS prev_close
            FROM ohlcv_raw
            WHERE exchange = 'binance' AND interval = '4h'
        )
        WHERE prev_close IS NOT NULL
          AND (close / prev_close > 3 OR close / prev_close < 0.33)
    """).fetchall()
    _log_dq_issues(con, result, now)
    checks_run += 1

    # 5. Assets with large OHLCV gaps (missing consecutive bars)
    result = con.execute("""
        SELECT asset, ts_open, 'time_gap' AS check_name, 'warn' AS severity,
               CONCAT('gap_hours=', gap_h) AS detail
        FROM (
            SELECT asset, ts_open,
                   ROUND((ts_open - LAG(ts_open) OVER (PARTITION BY asset ORDER BY ts_open)) / 3600.0, 1) AS gap_h
            FROM ohlcv_raw
            WHERE exchange = 'binance' AND interval = '4h'
        )
        WHERE gap_h IS NOT NULL AND gap_h > 8   -- more than 2 missing bars
    """).fetchall()
    _log_dq_issues(con, result, now)
    checks_run += 1

    log.info(f"Data quality checks complete ({checks_run} checks, {_total_issues} issues logged)")
    return _total_issues


_total_issues = 0

def _log_dq_issues(con, rows: list, now: int):
    global _total_issues
    if not rows:
        return
    _total_issues += len(rows)
    con.executemany("""
        INSERT OR REPLACE INTO data_quality_log
            (asset, ts_open, check_name, severity, detail, logged_at)
        VALUES (?, ?, ?, ?, ?, ?)
    """, [(r[0], r[1], r[2], r[3], r[4], now) for r in rows])
    con.commit()
    log.warning(f"  DQ: {len(rows)} rows flagged for '{rows[0][2]}'")


# ── Coverage Report ────────────────────────────────────────────────────────

def print_coverage_report(con):
    """Print a summary of what's in the database."""
    print("\n" + "─" * 65)
    print("  INGESTION COVERAGE REPORT")
    print("─" * 65)

    # OHLCV coverage
    ohlcv_summary = con.execute("""
        SELECT asset,
               COUNT(*) AS bars,
               MIN(DATE(TO_TIMESTAMP(ts_open))) AS earliest,
               MAX(DATE(TO_TIMESTAMP(ts_open))) AS latest
        FROM ohlcv_raw
        WHERE exchange = 'binance' AND interval = '4h'
        GROUP BY asset
        ORDER BY asset
    """).fetchdf()

    if not ohlcv_summary.empty:
        print("\n  OHLCV (4h bars):")
        print(ohlcv_summary.to_string(index=False))
    else:
        print("\n  OHLCV: No data yet")

    # Social post coverage
    social_summary = con.execute("""
        SELECT asset, platform, COUNT(*) AS posts,
               MIN(DATE(TO_TIMESTAMP(post_ts))) AS earliest,
               MAX(DATE(TO_TIMESTAMP(post_ts))) AS latest
        FROM social_posts_raw
        WHERE asset IS NOT NULL
        GROUP BY asset, platform
        ORDER BY asset, platform
    """).fetchdf()

    if not social_summary.empty:
        print("\n  Social Posts:")
        print(social_summary.to_string(index=False))
    else:
        print("\n  Social Posts: No data yet")

    # DQ issues
    dq_count = con.execute("SELECT COUNT(*) FROM data_quality_log").fetchone()[0]
    print(f"\n  Data Quality Issues: {dq_count}")
    print("─" * 65 + "\n")


# ── Main Orchestrator ──────────────────────────────────────────────────────

def run_ingestion(
    mode: str = "backfill",
    assets: list[str] = None,
    skip_ohlcv: bool = False,
    skip_reddit: bool = False,
    skip_twitter: bool = False,
):
    target_assets = assets or ASSET_UNIVERSE

    # Ensure DB is initialized
    con = init_db(DB_PATH)
    log.info(f"=== Ingestion Start | mode={mode} | assets={len(target_assets)} ===")

    # ── Step 1: OHLCV ──────────────────────────────────────────────────────
    if not skip_ohlcv:
        log.info("── Step 1: OHLCV Ingestion")
        ingester = OHLCVIngester(con, exchange_id=OHLCV_EXCHANGE, interval=OHLCV_INTERVAL)
        for asset in target_assets:
            try:
                if mode == "backfill":
                    ingester.backfill(asset, lookback_days=OHLCV_LOOKBACK_DAYS)
                else:
                    ingester.incremental(asset)
                sync_ohlcv_to_panel(con, asset)
            except Exception as e:
                log.error(f"[{asset}] OHLCV failed: {e}", exc_info=True)
    else:
        log.info("── Step 1: OHLCV skipped")

    # ── Step 2: Reddit ─────────────────────────────────────────────────────
    if not skip_reddit:
        log.info("── Step 2: Reddit Scrape")
        try:
            scraper = RedditScraper(con, target_assets)
            feed    = "top" if mode == "backfill" else "hot"

            # General subreddits
            for sub in REDDIT_SUBREDDITS:
                try:
                    scraper.scrape_subreddit(sub, feed=feed, limit=200)
                except Exception as e:
                    log.warning(f"r/{sub} failed: {e}")

            # Asset-specific subreddits
            asset_map = {a: ASSET_SUBREDDIT_MAP.get(a, []) for a in target_assets}
            scraper.scrape_asset_subreddits(asset_map, feed=feed, limit=100)

        except Exception as e:
            log.error(f"Reddit ingestion failed: {e}", exc_info=True)
    else:
        log.info("── Step 2: Reddit skipped")

    # ── Step 3: Twitter ────────────────────────────────────────────────────
    if not skip_twitter:
        log.info("── Step 3: Twitter/X Scrape")
        try:
            twitter = get_twitter_ingester(con, target_assets, mode="auto")
            if hasattr(twitter, 'backfill_all_assets'):
                # SnscrapeIngester
                keywords = {a: ASSET_TWITTER_KEYWORDS.get(a, [a]) for a in target_assets}
                lookback = OHLCV_LOOKBACK_DAYS if mode == "backfill" else 1
                twitter.backfill_all_assets(keywords, lookback_days=min(lookback, 30))
            else:
                # TweepyIngester — only recent supported on free tier
                for asset in target_assets:
                    kw = ASSET_TWITTER_KEYWORDS.get(asset, [asset])
                    twitter.scrape_recent(asset, kw, hours_back=4)
        except Exception as e:
            log.error(f"Twitter ingestion failed: {e}", exc_info=True)
    else:
        log.info("── Step 3: Twitter skipped")

    # ── Step 4: Data Quality ───────────────────────────────────────────────
    log.info("── Step 4: Data Quality Checks")
    global _total_issues
    _total_issues = 0
    run_data_quality_checks(con)

    # ── Step 5: Coverage Report ────────────────────────────────────────────
    print_coverage_report(con)

    log.info("=== Ingestion Complete ===")
    con.close()


# ── CLI ────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    logging.basicConfig(level=LOG_LEVEL, format=LOG_FORMAT)

    parser = argparse.ArgumentParser(description="Sentiment Engine — Data Ingestion")
    parser.add_argument("--mode",           choices=["backfill", "incremental"], default="backfill")
    parser.add_argument("--assets",         nargs="+", help="Subset of assets to ingest")
    parser.add_argument("--skip-ohlcv",     action="store_true")
    parser.add_argument("--skip-reddit",    action="store_true")
    parser.add_argument("--skip-twitter",   action="store_true")
    args = parser.parse_args()

    run_ingestion(
        mode=args.mode,
        assets=args.assets,
        skip_ohlcv=args.skip_ohlcv,
        skip_reddit=args.skip_reddit,
        skip_twitter=args.skip_twitter,
    )
