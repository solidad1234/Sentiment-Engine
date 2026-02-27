"""
tests/test_schema.py
─────────────────────
Validates DuckDB schema creation and basic insert/query roundtrip.
No external API calls. Uses in-memory DuckDB.

Run: python -m pytest sentiment_engine/tests/test_schema.py -v
  or: python sentiment_engine/tests/test_schema.py
"""

import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parents[2]))

import duckdb
from sentiment_engine.storage.schema import init_db


def get_test_db():
    """Create an in-memory DB for testing."""
    import tempfile, os
    tmp = Path(tempfile.mktemp(suffix=".duckdb"))
    return init_db(tmp), tmp


def test_schema_creation():
    con, tmp = get_test_db()
    tables = {r[0] for r in con.execute("SHOW TABLES").fetchall()}
    expected = {
        "ohlcv_raw", "social_posts_raw", "scoring_runs",
        "post_sentiment", "sentiment_bars", "research_panel", "data_quality_log"
    }
    assert expected.issubset(tables), f"Missing tables: {expected - tables}"
    print("✓ All tables created")
    con.close()
    tmp.unlink(missing_ok=True)


def test_ohlcv_insert_and_query():
    con, tmp = get_test_db()
    now = int(time.time())

    con.execute("""
        INSERT INTO ohlcv_raw
            (asset, exchange, interval, ts_open, ts_close,
             open, high, low, close, volume, volume_usd, trade_count, ingested_at)
        VALUES
            ('BTC', 'binance', '4h', 1700000000, 1700014399, 37000, 37500, 36900, 37200, 100.5, 3729450, 5000, ?)
    """, [now])

    row = con.execute("SELECT * FROM ohlcv_raw WHERE asset = 'BTC'").fetchone()
    assert row is not None
    assert row[0] == 'BTC'
    assert row[6] == 37500  # high
    print("✓ OHLCV insert/query roundtrip OK")
    con.close()
    tmp.unlink(missing_ok=True)


def test_social_posts_insert():
    con, tmp = get_test_db()
    now = int(time.time())

    con.execute("""
        INSERT INTO social_posts_raw
            (post_id, platform, asset, source, post_ts, text_body,
             author, upvotes, comments, shares, is_original, ingested_at)
        VALUES ('abc123', 'reddit', 'BTC', 'CryptoCurrency', ?, 'BTC is going to the moon! Very bullish on Bitcoin.', 'user1', 42, 7, 0, TRUE, ?)
    """, [now - 3600, now])

    count = con.execute("SELECT COUNT(*) FROM social_posts_raw WHERE asset = 'BTC'").fetchone()[0]
    assert count == 1
    print("✓ Social posts insert OK")
    con.close()
    tmp.unlink(missing_ok=True)


def test_primary_key_dedup():
    """Inserting duplicate post_id should be silently ignored."""
    con, tmp = get_test_db()
    now = int(time.time())

    for _ in range(3):  # insert same post 3 times
        con.execute("""
            INSERT OR IGNORE INTO social_posts_raw
                (post_id, platform, asset, source, post_ts, text_body,
                 author, upvotes, comments, shares, is_original, ingested_at)
            VALUES ('dup_id', 'reddit', 'ETH', 'ethereum', ?, 'Ethereum is great for defi applications', 'u2', 10, 2, 0, TRUE, ?)
        """, [now, now])

    count = con.execute("SELECT COUNT(*) FROM social_posts_raw WHERE post_id = 'dup_id'").fetchone()[0]
    assert count == 1, f"Expected 1, got {count}"
    print("✓ Deduplication via INSERT OR IGNORE OK")
    con.close()
    tmp.unlink(missing_ok=True)


def test_research_panel_insert_and_view():
    con, tmp = get_test_db()
    now = int(time.time())

    con.execute("""
        INSERT INTO research_panel
            (asset, ts_open, open, high, low, close, volume, has_price, has_sentiment,
             sentiment_quality, updated_at)
        VALUES
            ('BTC', 1700000000, 37000, 37500, 36900, 37200, 100, TRUE, FALSE, 'missing', ?),
            ('ETH', 1700000000, 2000, 2050, 1990, 2020, 500, TRUE, FALSE, 'missing', ?)
    """, [now, now])

    # Test coverage view
    summary = con.execute("SELECT * FROM v_coverage_summary").fetchdf()
    assert len(summary) == 2
    assert set(summary['asset'].tolist()) == {'BTC', 'ETH'}
    print("✓ Research panel insert + coverage view OK")
    con.close()
    tmp.unlink(missing_ok=True)


def test_column_names():
    """Verify research_panel has all expected signal columns."""
    con, tmp = get_test_db()
    cols = {r[0] for r in con.execute("DESCRIBE research_panel").fetchall()}
    required_signal_cols = {
        "sentiment_velocity_1", "sentiment_velocity_3", "sentiment_velocity_6",
        "sentiment_accel_1", "bull_ratio", "bear_ratio", "controversy_score",
        "ret_1bar", "ret_3bar", "ret_6bar", "ret_12bar",
        "price_momentum_1", "price_momentum_6", "realized_vol_6",
    }
    missing = required_signal_cols - cols
    assert not missing, f"Missing signal columns: {missing}"
    print(f"✓ All {len(required_signal_cols)} signal columns present in research_panel")
    con.close()
    tmp.unlink(missing_ok=True)


if __name__ == "__main__":
    print("\n── Running Schema Tests ──\n")
    test_schema_creation()
    test_ohlcv_insert_and_query()
    test_social_posts_insert()
    test_primary_key_dedup()
    test_research_panel_insert_and_view()
    test_column_names()
    print("\n✓ All tests passed\n")
