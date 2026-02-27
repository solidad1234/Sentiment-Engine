"""
storage/schema.py
─────────────────
Initializes the DuckDB research database.

Design philosophy:
  - DuckDB for columnar OLAP queries (fast window functions, aggregations)
  - Separate raw and derived layers — never mutate raw data
  - All timestamps stored as UTC epoch seconds (INTEGER) for portability
  - Nullable sentiment fields — missing data is valid, not an error

Tables:
  RAW LAYER
  ├── ohlcv_raw          — price bars from exchange
  ├── social_posts_raw   — individual posts/tweets before scoring
  └── scoring_runs       — audit log of sentiment model runs

  DERIVED LAYER
  ├── sentiment_bars     — sentiment aggregated to 4h bars (per asset)
  ├── research_panel     — joined OHLCV + sentiment, the analysis table
  └── data_quality_log   — per-bar data quality flags
"""

import duckdb
import logging
from pathlib import Path

log = logging.getLogger(__name__)


DDL_STATEMENTS = [

    # ── RAW: OHLCV ────────────────────────────────────────────────────────
    """
    CREATE TABLE IF NOT EXISTS ohlcv_raw (
        asset           VARCHAR     NOT NULL,       -- e.g. 'BTC'
        exchange        VARCHAR     NOT NULL,       -- e.g. 'binance'
        interval        VARCHAR     NOT NULL,       -- e.g. '4h'
        ts_open         INTEGER     NOT NULL,       -- bar open UTC epoch seconds
        ts_close        INTEGER     NOT NULL,       -- bar close UTC epoch seconds
        open            DOUBLE      NOT NULL,
        high            DOUBLE      NOT NULL,
        low             DOUBLE      NOT NULL,
        close           DOUBLE      NOT NULL,
        volume          DOUBLE      NOT NULL,       -- base asset volume
        volume_usd      DOUBLE,                    -- quote (USDT) volume
        trade_count     INTEGER,                   -- number of trades in bar
        ingested_at     INTEGER     NOT NULL,       -- when we pulled this row
        PRIMARY KEY (asset, exchange, interval, ts_open)
    )
    """,

    # ── RAW: Social Posts ─────────────────────────────────────────────────
    """
    CREATE TABLE IF NOT EXISTS social_posts_raw (
        post_id         VARCHAR     NOT NULL,       -- platform-native id
        platform        VARCHAR     NOT NULL,       -- 'reddit' | 'twitter' | 'telegram'
        asset           VARCHAR,                   -- NULL if not asset-specific
        source          VARCHAR,                   -- subreddit name, account handle, etc.
        post_ts         INTEGER     NOT NULL,       -- UTC epoch seconds of post creation
        text_body       TEXT        NOT NULL,       -- raw text (title + body for Reddit)
        author          VARCHAR,
        upvotes         INTEGER     DEFAULT 0,
        comments        INTEGER     DEFAULT 0,
        shares          INTEGER     DEFAULT 0,      -- retweets / forwards
        is_original     BOOLEAN     DEFAULT TRUE,   -- False if reply/comment
        ingested_at     INTEGER     NOT NULL,
        PRIMARY KEY (platform, post_id)
    )
    """,

    # ── RAW: Scoring Audit Log ────────────────────────────────────────────
    """
    CREATE TABLE IF NOT EXISTS scoring_runs (
        run_id          VARCHAR     NOT NULL PRIMARY KEY,
        model_name      VARCHAR     NOT NULL,       -- 'finbert' | 'vader' | 'llm'
        model_version   VARCHAR,
        started_at      INTEGER     NOT NULL,
        completed_at    INTEGER,
        posts_scored    INTEGER,
        errors          INTEGER,
        params_json     TEXT        -- JSON blob of model hyperparams
    )
    """,

    # ── DERIVED: Sentiment Scores on Posts ───────────────────────────────
    """
    CREATE TABLE IF NOT EXISTS post_sentiment (
        platform        VARCHAR     NOT NULL,
        post_id         VARCHAR     NOT NULL,
        run_id          VARCHAR     NOT NULL,       -- FK → scoring_runs
        score_positive  DOUBLE,                    -- [0,1] prob positive
        score_negative  DOUBLE,                    -- [0,1] prob negative
        score_neutral   DOUBLE,                    -- [0,1] prob neutral
        compound        DOUBLE,                    -- [-1, +1] scalar
        scored_at       INTEGER     NOT NULL,
        PRIMARY KEY (platform, post_id, run_id)
    )
    """,

    # ── DERIVED: Sentiment Bars (aggregated to 4h) ────────────────────────
    """
    CREATE TABLE IF NOT EXISTS sentiment_bars (
        asset               VARCHAR     NOT NULL,
        ts_open             INTEGER     NOT NULL,   -- aligns with ohlcv_raw.ts_open
        platform            VARCHAR     NOT NULL,   -- 'reddit' | 'twitter' | 'all'
        model_name          VARCHAR     NOT NULL,

        -- Volume metrics
        post_count          INTEGER,               -- raw post count
        weighted_post_count DOUBLE,               -- engagement-weighted post count

        -- Sentiment level
        sentiment_mean      DOUBLE,               -- mean compound score
        sentiment_median    DOUBLE,
        sentiment_std       DOUBLE,
        sentiment_weighted  DOUBLE,               -- upvote-weighted mean compound

        -- Sentiment velocity (populated by feature engineering step)
        sentiment_velocity_1  DOUBLE,             -- ΔS over 1 bar (4h)
        sentiment_velocity_3  DOUBLE,             -- ΔS over 3 bars (12h)
        sentiment_velocity_6  DOUBLE,             -- ΔS over 6 bars (24h)

        -- Sentiment acceleration
        sentiment_accel_1   DOUBLE,               -- Δvelocity_1 over 1 bar

        -- Dispersion / controversy
        bull_ratio          DOUBLE,               -- % positive posts
        bear_ratio          DOUBLE,               -- % negative posts
        controversy_score   DOUBLE,               -- bull_ratio * bear_ratio * 4 → [0,1]

        computed_at         INTEGER     NOT NULL,
        PRIMARY KEY (asset, ts_open, platform, model_name)
    )
    """,

    # ── DERIVED: Research Panel ───────────────────────────────────────────
    # The single table used for all statistical analysis.
    # One row = one (asset, 4h bar) observation.
    """
    CREATE TABLE IF NOT EXISTS research_panel (
        asset               VARCHAR     NOT NULL,
        ts_open             INTEGER     NOT NULL,

        -- Price
        open                DOUBLE,
        high                DOUBLE,
        low                 DOUBLE,
        close               DOUBLE,
        volume              DOUBLE,
        volume_usd          DOUBLE,

        -- Returns (forward-looking — populated after bar closes)
        ret_1bar            DOUBLE,               -- 4h forward log return
        ret_3bar            DOUBLE,               -- 12h forward log return
        ret_6bar            DOUBLE,               -- 24h forward log return
        ret_12bar           DOUBLE,               -- 48h forward log return

        -- Price-derived features
        log_volume          DOUBLE,
        price_momentum_1    DOUBLE,               -- 1-bar price return
        price_momentum_6    DOUBLE,               -- 6-bar price return
        realized_vol_6      DOUBLE,               -- 6-bar rolling std of returns
        bar_range           DOUBLE,               -- (high - low) / open

        -- Sentiment (from sentiment_bars, platform='all')
        post_count          INTEGER,
        sentiment_mean      DOUBLE,
        sentiment_weighted  DOUBLE,
        sentiment_velocity_1  DOUBLE,
        sentiment_velocity_3  DOUBLE,
        sentiment_velocity_6  DOUBLE,
        sentiment_accel_1   DOUBLE,
        bull_ratio          DOUBLE,
        bear_ratio          DOUBLE,
        controversy_score   DOUBLE,

        -- Data quality
        has_price           BOOLEAN     DEFAULT FALSE,
        has_sentiment       BOOLEAN     DEFAULT FALSE,
        sentiment_quality   VARCHAR,              -- 'high' | 'low' | 'missing'

        updated_at          INTEGER     NOT NULL,
        PRIMARY KEY (asset, ts_open)
    )
    """,

    # ── DERIVED: Data Quality Log ─────────────────────────────────────────
    """
    CREATE TABLE IF NOT EXISTS data_quality_log (
        asset           VARCHAR     NOT NULL,
        ts_open         INTEGER     NOT NULL,
        check_name      VARCHAR     NOT NULL,
        severity        VARCHAR     NOT NULL,       -- 'warn' | 'error'
        detail          TEXT,
        logged_at       INTEGER     NOT NULL,
        PRIMARY KEY (asset, ts_open, check_name)
    )
    """,

    # ── INDEXES ───────────────────────────────────────────────────────────
    "CREATE INDEX IF NOT EXISTS idx_ohlcv_asset_ts     ON ohlcv_raw(asset, ts_open)",
    "CREATE INDEX IF NOT EXISTS idx_posts_asset_ts     ON social_posts_raw(asset, post_ts)",
    "CREATE INDEX IF NOT EXISTS idx_posts_platform     ON social_posts_raw(platform, post_ts)",
    "CREATE INDEX IF NOT EXISTS idx_sentbars_asset_ts  ON sentiment_bars(asset, ts_open)",
    "CREATE INDEX IF NOT EXISTS idx_panel_asset_ts     ON research_panel(asset, ts_open)",

]


# ── Convenience views ──────────────────────────────────────────────────────

VIEW_STATEMENTS = [

    # Latest price for each asset
    """
    CREATE OR REPLACE VIEW v_latest_ohlcv AS
    SELECT *
    FROM ohlcv_raw
    QUALIFY ROW_NUMBER() OVER (PARTITION BY asset ORDER BY ts_open DESC) = 1
    """,

    # Panel rows with both price AND sentiment — the clean analysis set
    """
    CREATE OR REPLACE VIEW v_clean_panel AS
    SELECT *
    FROM research_panel
    WHERE has_price = TRUE
      AND has_sentiment = TRUE
      AND sentiment_quality = 'high'
    """,

    # Data coverage summary — useful for QA
    """
    CREATE OR REPLACE VIEW v_coverage_summary AS
    SELECT
        asset,
        COUNT(*)                                        AS total_bars,
        SUM(has_price::INT)                             AS bars_with_price,
        SUM(has_sentiment::INT)                         AS bars_with_sentiment,
        SUM((has_price AND has_sentiment)::INT)         AS bars_complete,
        ROUND(AVG(post_count), 1)                       AS avg_posts_per_bar,
        MIN(ts_open)                                    AS earliest_bar,
        MAX(ts_open)                                    AS latest_bar
    FROM research_panel
    GROUP BY asset
    ORDER BY asset
    """,
]


def init_db(db_path: Path) -> duckdb.DuckDBPyConnection:
    """
    Create (or connect to) the research DuckDB, apply all DDL.
    Safe to call multiple times — all statements are idempotent.
    """
    log.info(f"Initializing DuckDB at {db_path}")
    con = duckdb.connect(str(db_path))

    con.execute("PRAGMA threads=4")
    con.execute("PRAGMA memory_limit='2GB'")

    for stmt in DDL_STATEMENTS:
        con.execute(stmt)
    log.info(f"  ✓ {len(DDL_STATEMENTS)} DDL statements applied")

    for stmt in VIEW_STATEMENTS:
        con.execute(stmt)
    log.info(f"  ✓ {len(VIEW_STATEMENTS)} views created/replaced")

    con.commit()
    log.info("Database ready.")
    return con


def get_connection(db_path: Path) -> duckdb.DuckDBPyConnection:
    """Return a connection to an already-initialized database."""
    return duckdb.connect(str(db_path))


if __name__ == "__main__":
    import sys
    sys.path.insert(0, str(Path(__file__).parents[2]))
    from sentiment_engine.config.settings import DB_PATH
    logging.basicConfig(level="INFO", format="%(asctime)s | %(levelname)s | %(message)s")
    con = init_db(DB_PATH)
    print("\n── Tables ──")
    print(con.execute("SHOW TABLES").fetchdf().to_string(index=False))
    con.close()
