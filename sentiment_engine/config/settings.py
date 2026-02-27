"""
Phase 1 Research Engine — Global Configuration
"""
from pathlib import Path

# ── Paths ──────────────────────────────────────────────────────────────────
ROOT_DIR   = Path(__file__).resolve().parents[1]
DATA_DIR   = ROOT_DIR / "data"
DB_PATH    = DATA_DIR / "research.duckdb"
RAW_DIR    = DATA_DIR / "raw"
LOG_DIR    = ROOT_DIR / "logs"

for _d in [DATA_DIR, RAW_DIR, LOG_DIR]:
    _d.mkdir(parents=True, exist_ok=True)

# ── Asset Universe (Top 20 by market cap, symbols used across exchanges) ───
ASSET_UNIVERSE = [
    "BTC", "ETH", "BNB", "SOL", "XRP",
    "DOGE", "ADA", "AVAX", "SHIB", "TON",
    "LINK", "DOT", "POL", "LTC", "BCH",
    "UNI", "ATOM", "XLM", "ICP", "FIL",
]

# Trading pairs (USDT quoted, Binance convention)
TRADING_PAIRS = [f"{a}USDT" for a in ASSET_UNIVERSE]

# ── OHLCV Config ───────────────────────────────────────────────────────────
OHLCV_INTERVAL     = "4h"          # bar size
OHLCV_EXCHANGE     = "binance"     # ccxt exchange id
OHLCV_LOOKBACK_DAYS = 365          # 1 year of history for Phase 1

# ── Social Data Config ─────────────────────────────────────────────────────
REDDIT_SUBREDDITS = [
    "CryptoCurrency", "Bitcoin", "ethereum", "SatoshiStreetBets",
    "altcoin", "CryptoMarkets", "solana", "cardano", "dogecoin",
]

# Map asset → relevant Reddit communities for targeted scraping
ASSET_SUBREDDIT_MAP = {
    "BTC":   ["Bitcoin", "CryptoCurrency"],
    "ETH":   ["ethereum", "CryptoCurrency"],
    "SOL":   ["solana", "CryptoCurrency"],
    "ADA":   ["cardano"],
    "DOGE":  ["dogecoin", "SatoshiStreetBets"],
    "LINK":  ["Chainlink"],
    "DOT":   ["dot"],
    "POL": ["0xPolygon"],
    "AVAX":  ["Avax"],
    "UNI":   ["UniSwap"],
}

# Twitter/X search keywords per asset
ASSET_TWITTER_KEYWORDS = {
    asset: [f"#{asset}", f"${asset}", asset]
    for asset in ASSET_UNIVERSE
}

# ── Sentiment Scoring ──────────────────────────────────────────────────────
SENTIMENT_MODEL     = "finbert"    # options: "finbert" | "vader" | "llm"
SENTIMENT_WINDOW_H  = 4            # aggregate sentiment over N hours (matches bar)
SENTIMENT_MIN_CHARS = 20           # filter very short posts

# ── Data Quality ──────────────────────────────────────────────────────────
MIN_POSTS_PER_WINDOW = 5           # minimum posts to compute valid sentiment
OUTLIER_ZSCORE_THRESH = 4.0        # flag price/sentiment outliers beyond N std

# ── Logging ───────────────────────────────────────────────────────────────
LOG_LEVEL = "INFO"
LOG_FORMAT = "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"
