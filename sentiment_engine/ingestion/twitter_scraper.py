"""
ingestion/twitter_scraper.py
─────────────────────────────
Dual-mode Twitter/X ingestion:

  Mode A — snscrape (no API key, good for historical backfill, slower)
  Mode B — Twitter API v2 via tweepy (requires Bearer Token, for live/recent)

Both write identical rows to social_posts_raw.

Environment variables (Mode B only):
    TWITTER_BEARER_TOKEN

Install:
    pip install snscrape tweepy
"""

import os
import time
import logging
from datetime import datetime, timezone, timedelta
from typing import Optional

log = logging.getLogger(__name__)
PLATFORM = "twitter"


def _epoch_now() -> int:
    return int(datetime.now(timezone.utc).timestamp())


# ── Mode A: snscrape (historical, no auth) ─────────────────────────────────

class SnscrapeIngester:
    """
    Uses snscrape to pull tweets without an API key.
    Good for backfilling 30-180 days of history.
    Slower (~1-3 tweets/sec). Twitter may block heavy use.
    """

    SLEEP_BETWEEN_ASSETS = 2.0

    def __init__(self, con, asset_universe: list[str]):
        try:
            import snscrape.modules.twitter as sntwitter
            self._sntwitter = sntwitter
        except ImportError:
            raise ImportError("Install snscrape: pip install snscrape")

        self.con            = con
        self.asset_universe = asset_universe

    def scrape_asset(
        self,
        asset: str,
        keywords: list[str],
        since_dt: datetime,
        until_dt: Optional[datetime] = None,
        max_tweets: int = 1000,
        min_likes: int = 2,
    ) -> int:
        until_dt  = until_dt or datetime.now(timezone.utc)
        since_str = since_dt.strftime("%Y-%m-%d")
        until_str = until_dt.strftime("%Y-%m-%d")

        # Build search query
        kw_clause = " OR ".join(f'"{kw}"' for kw in keywords)
        query     = f"({kw_clause}) since:{since_str} until:{until_str} lang:en -is:retweet"

        log.info(f"[{asset}] snscrape query: {query[:80]}...")
        rows = []

        try:
            scraper = self._sntwitter.TwitterSearchScraper(query)
            for i, tweet in enumerate(scraper.get_items()):
                if i >= max_tweets:
                    break
                if tweet.likeCount is not None and tweet.likeCount < min_likes:
                    continue

                row = self._parse_tweet(tweet, asset)
                if row:
                    rows.append(row)
        except Exception as e:
            log.error(f"[{asset}] snscrape error: {e}")

        return self._insert_rows(rows)

    def backfill_all_assets(
        self,
        asset_keywords: dict,
        lookback_days: int = 30,
        max_tweets_per_asset: int = 2000,
    ) -> dict[str, int]:
        since_dt = datetime.now(timezone.utc) - timedelta(days=lookback_days)
        results  = {}

        for asset, keywords in asset_keywords.items():
            try:
                n = self.scrape_asset(
                    asset, keywords, since_dt,
                    max_tweets=max_tweets_per_asset,
                )
                results[asset] = n
                time.sleep(self.SLEEP_BETWEEN_ASSETS)
            except Exception as e:
                log.error(f"[{asset}] Failed: {e}")
                results[asset] = 0

        return results

    def _parse_tweet(self, tweet, asset: str) -> Optional[tuple]:
        try:
            text = tweet.rawContent or tweet.content or ""
            if len(text) < 20:
                return None

            return (
                str(tweet.id),
                PLATFORM,
                asset,
                tweet.user.username if tweet.user else None,
                int(tweet.date.timestamp()),
                text[:1000],
                tweet.user.username if tweet.user else None,
                tweet.likeCount or 0,
                tweet.replyCount or 0,
                tweet.retweetCount or 0,
                True,
                _epoch_now(),
            )
        except Exception as e:
            log.debug(f"Tweet parse error: {e}")
            return None

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
        log.info(f"Inserted {len(rows)} tweets (snscrape)")
        return len(rows)


# ── Mode B: Twitter API v2 via tweepy (live/recent) ──────────────────────

class TweepyIngester:
    """
    Uses Twitter API v2 (Bearer Token) for recent tweets (last 7 days free tier).
    Higher quality metadata (impression counts, etc.) but requires API access.
    """

    MAX_RESULTS_PER_REQUEST = 100   # API max per page

    def __init__(self, con, asset_universe: list[str]):
        try:
            import tweepy
        except ImportError:
            raise ImportError("Install tweepy: pip install tweepy")

        bearer_token = os.environ.get("TWITTER_BEARER_TOKEN")
        if not bearer_token:
            raise EnvironmentError("TWITTER_BEARER_TOKEN not set")

        self.con            = con
        self.asset_universe = asset_universe
        self.client         = tweepy.Client(bearer_token=bearer_token, wait_on_rate_limit=True)
        log.info("TweepyIngester ready (API v2)")

    def scrape_recent(
        self,
        asset: str,
        keywords: list[str],
        hours_back: int = 4,
        max_results: int = 500,
    ) -> int:
        """
        Fetch tweets from the last N hours for a given asset.
        Designed for incremental 4h-bar updates.
        """
        import tweepy
        from datetime import timezone

        start_time = datetime.now(timezone.utc) - timedelta(hours=hours_back)
        kw_clause  = " OR ".join(keywords[:5])  # API limits query length
        query      = f"({kw_clause}) lang:en -is:retweet"

        rows     = []
        paginator = tweepy.Paginator(
            self.client.search_recent_tweets,
            query=query,
            start_time=start_time,
            tweet_fields=["created_at", "public_metrics", "author_id"],
            max_results=min(self.MAX_RESULTS_PER_REQUEST, max_results),
            limit=max_results // self.MAX_RESULTS_PER_REQUEST + 1,
        )

        for tweet in paginator.flatten(limit=max_results):
            row = self._parse_tweet(tweet, asset)
            if row:
                rows.append(row)

        return self._insert_rows(rows)

    def _parse_tweet(self, tweet, asset: str) -> Optional[tuple]:
        try:
            metrics = tweet.public_metrics or {}
            return (
                str(tweet.id),
                PLATFORM,
                asset,
                str(tweet.author_id),
                int(tweet.created_at.timestamp()),
                (tweet.text or "")[:1000],
                str(tweet.author_id),
                metrics.get("like_count", 0),
                metrics.get("reply_count", 0),
                metrics.get("retweet_count", 0),
                True,
                _epoch_now(),
            )
        except Exception as e:
            log.debug(f"Tweet parse error: {e}")
            return None

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
        log.info(f"Inserted {len(rows)} tweets (tweepy)")
        return len(rows)


# ── Factory ────────────────────────────────────────────────────────────────

def get_twitter_ingester(con, asset_universe: list[str], mode: str = "auto"):
    """
    Return the best available Twitter ingester.
    mode: 'auto' | 'snscrape' | 'tweepy'
    """
    if mode == "tweepy" or (mode == "auto" and os.environ.get("TWITTER_BEARER_TOKEN")):
        try:
            return TweepyIngester(con, asset_universe)
        except Exception as e:
            log.warning(f"Tweepy unavailable ({e}), falling back to snscrape")

    return SnscrapeIngester(con, asset_universe)
