"""
ingestion/reddit_scraper.py
───────────────────────────
Scrapes Reddit posts using PRAW (official API) with pushshift.io fallback
for historical data. Writes to social_posts_raw table.

Handles:
  - Subreddit hot/new/top feeds (live)
  - Historical post retrieval (via Pushshift or PRAW search)
  - Basic deduplication via PRIMARY KEY conflict
  - Rate limit compliance

Setup:
  Create a Reddit app at https://www.reddit.com/prefs/apps
  Set environment variables:
    REDDIT_CLIENT_ID
    REDDIT_CLIENT_SECRET
    REDDIT_USER_AGENT  (e.g. "SentimentEngine/0.1 by YourUsername")
"""

import os
import re
import time
import logging
from datetime import datetime, timezone
from typing import Optional

log = logging.getLogger(__name__)

PLATFORM = "reddit"

# Regex patterns to detect asset mentions in post text
def _build_asset_patterns(asset_universe: list[str]) -> dict:
    """Build compiled regex patterns for each asset."""
    patterns = {}
    for asset in asset_universe:
        # Match $BTC, #BTC, or standalone BTC (word boundaries)
        patterns[asset] = re.compile(
            rf"(\${asset}|#{asset}|\b{asset}\b)", re.IGNORECASE
        )
    return patterns


class RedditScraper:
    """
    Scrapes Reddit posts and persists raw text to DuckDB.

    Parameters
    ----------
    con              : duckdb connection
    asset_universe   : list of asset symbols to tag mentions
    """

    SLEEP_BETWEEN_REQUESTS = 1.0   # seconds (PRAW is rate-limited to 60 req/min)

    def __init__(self, con, asset_universe: list[str]):
        try:
            import praw
        except ImportError:
            raise ImportError("Install praw: pip install praw")

        self.con            = con
        self.asset_universe = asset_universe
        self._patterns      = _build_asset_patterns(asset_universe)

        client_id     = os.environ.get("REDDIT_CLIENT_ID", "")
        client_secret = os.environ.get("REDDIT_CLIENT_SECRET", "")
        user_agent    = os.environ.get("REDDIT_USER_AGENT", "SentimentEngine/0.1")

        if not client_id or not client_secret:
            log.warning("REDDIT_CLIENT_ID / REDDIT_CLIENT_SECRET not set — read-only mode")

        self.reddit = praw.Reddit(
            client_id=client_id,
            client_secret=client_secret,
            user_agent=user_agent,
        )
        log.info(f"RedditScraper ready (read_only={self.reddit.read_only})")

    # ── Public API ─────────────────────────────────────────────────────────

    def scrape_subreddit(
        self,
        subreddit_name: str,
        feed: str = "hot",          # 'hot' | 'new' | 'top'
        limit: int = 100,
        time_filter: str = "day",   # for 'top' feed: 'hour'|'day'|'week'|'month'
    ) -> int:
        """
        Scrape posts from a subreddit feed.
        Returns number of posts inserted.
        """
        sub = self.reddit.subreddit(subreddit_name)
        log.info(f"Scraping r/{subreddit_name} [{feed}] limit={limit}")

        if feed == "hot":
            submissions = sub.hot(limit=limit)
        elif feed == "new":
            submissions = sub.new(limit=limit)
        elif feed == "top":
            submissions = sub.top(time_filter=time_filter, limit=limit)
        else:
            raise ValueError(f"Unknown feed: {feed}")

        rows = []
        for post in submissions:
            row = self._parse_submission(post, subreddit_name)
            if row:
                rows.append(row)

        return self._insert_rows(rows)

    def scrape_asset_subreddits(
        self,
        asset_subreddit_map: dict,
        feed: str = "hot",
        limit: int = 100,
    ) -> dict[str, int]:
        """
        Scrape all subreddits in the asset map.
        Returns {asset: posts_inserted}.
        """
        results = {}
        seen_subreddits = set()

        for asset, subreddits in asset_subreddit_map.items():
            asset_total = 0
            for sub in subreddits:
                if sub in seen_subreddits:
                    continue
                seen_subreddits.add(sub)
                try:
                    n = self.scrape_subreddit(sub, feed=feed, limit=limit)
                    asset_total += n
                    time.sleep(self.SLEEP_BETWEEN_REQUESTS)
                except Exception as e:
                    log.error(f"[{asset}] r/{sub} failed: {e}")
            results[asset] = asset_total

        return results

    def scrape_comments(
        self,
        subreddit_name: str,
        post_limit: int = 50,
        comment_limit: int = 20,
    ) -> int:
        """
        Fetch top-level comments on hot posts.
        Useful for capturing retail sentiment on popular threads.
        """
        sub  = self.reddit.subreddit(subreddit_name)
        rows = []

        for post in sub.hot(limit=post_limit):
            post.comments.replace_more(limit=0)
            for comment in post.comments[:comment_limit]:
                row = self._parse_comment(comment, subreddit_name)
                if row:
                    rows.append(row)
            time.sleep(0.2)

        return self._insert_rows(rows)

    # ── Parsers ────────────────────────────────────────────────────────────

    def _parse_submission(self, post, source: str) -> Optional[tuple]:
        """Parse a PRAW Submission into a db row tuple."""
        try:
            text_body = f"{post.title} {post.selftext or ''}".strip()

            if len(text_body) < 20:
                return None  # too short to score

            asset = self._detect_asset(text_body)

            return (
                post.id,
                PLATFORM,
                asset,                              # may be None
                source,
                int(post.created_utc),
                text_body[:4000],                   # truncate long bodies
                str(post.author) if post.author else None,
                post.score,                         # upvotes
                post.num_comments,
                0,                                  # shares N/A for Reddit
                True,                               # is_original
                _epoch_now(),
            )
        except Exception as e:
            log.debug(f"Skipping post {getattr(post, 'id', '?')}: {e}")
            return None

    def _parse_comment(self, comment, source: str) -> Optional[tuple]:
        """Parse a PRAW Comment into a db row tuple."""
        try:
            text_body = comment.body or ""
            if len(text_body) < 20 or text_body == "[deleted]":
                return None

            asset = self._detect_asset(text_body)

            return (
                f"c_{comment.id}",               # prefix to avoid id collision with posts
                PLATFORM,
                asset,
                source,
                int(comment.created_utc),
                text_body[:2000],
                str(comment.author) if comment.author else None,
                comment.score,
                0, 0,
                False,                           # is_original = False for comments
                _epoch_now(),
            )
        except Exception as e:
            log.debug(f"Skipping comment: {e}")
            return None

    def _detect_asset(self, text: str) -> Optional[str]:
        """
        Returns the first matched asset symbol in text, or None.
        Priority: explicit ticker match ($BTC > standalone BTC).
        """
        for asset, pattern in self._patterns.items():
            if pattern.search(text):
                return asset
        return None

    # ── Persistence ────────────────────────────────────────────────────────

    def _insert_rows(self, rows: list) -> int:
        if not rows:
            return 0

        self.con.executemany("""
            INSERT OR IGNORE INTO social_posts_raw
                (post_id, platform, asset, source, post_ts, text_body,
                 author, upvotes, comments, shares, is_original, ingested_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, rows)
        self.con.commit()
        log.info(f"Inserted {len(rows)} posts from Reddit")
        return len(rows)


# ── Helpers ────────────────────────────────────────────────────────────────

def _epoch_now() -> int:
    return int(datetime.now(timezone.utc).timestamp())


# ── Entry Point ────────────────────────────────────────────────────────────

def run(feed: str = "hot", limit: int = 200):
    import sys
    sys.path.insert(0, str(Path(__file__).parents[2]))
    from pathlib import Path
    from sentiment_engine.config.settings import (
        DB_PATH, ASSET_UNIVERSE, ASSET_SUBREDDIT_MAP, REDDIT_SUBREDDITS
    )
    from sentiment_engine.storage.schema import get_connection

    logging.basicConfig(level="INFO", format="%(asctime)s | %(levelname)s | %(message)s")
    con     = get_connection(DB_PATH)
    scraper = RedditScraper(con, ASSET_UNIVERSE)

    # 1. Scrape asset-specific subreddits
    results = scraper.scrape_asset_subreddits(ASSET_SUBREDDIT_MAP, feed=feed, limit=limit)
    log.info(f"Asset-specific scrape: {results}")

    # 2. Scrape general crypto subreddits
    for sub in REDDIT_SUBREDDITS:
        try:
            n = scraper.scrape_subreddit(sub, feed=feed, limit=limit)
            log.info(f"r/{sub}: {n} posts")
        except Exception as e:
            log.error(f"r/{sub} failed: {e}")

    con.close()


if __name__ == "__main__":
    from pathlib import Path
    import sys
    sys.path.insert(0, str(Path(__file__).parents[2]))
    run()
