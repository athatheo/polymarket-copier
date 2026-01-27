"""
Configuration for Polymarket Copy Trading Bot.

All settings are loaded from environment variables with sensible defaults.
"""

import os
from dotenv import load_dotenv

load_dotenv()

# =============================================================================
# Target Account
# =============================================================================
TARGET_WALLET: str = os.getenv("TARGET_WALLET", "")  # Required: wallet address to copy

# =============================================================================
# Your Wallet
# =============================================================================
PRIVATE_KEY: str = os.getenv("PRIVATE_KEY", "")

# =============================================================================
# Polymarket API Endpoints
# =============================================================================
GAMMA_API_URL: str = "https://gamma-api.polymarket.com"
DATA_API_URL: str = "https://data-api.polymarket.com"
CLOB_API_URL: str = "https://clob.polymarket.com"
CHAIN_ID: int = 137  # Polygon mainnet

# =============================================================================
# Polling & Timing
# =============================================================================
POLL_INTERVAL_SECONDS: int = int(os.getenv("POLL_INTERVAL_SECONDS", "10"))
# On fresh start (empty database), look back this many hours for recent trades
LOOKBACK_HOURS: int = int(os.getenv("LOOKBACK_HOURS", "24"))

# =============================================================================
# Safety Limits
# =============================================================================
MAX_SLIPPAGE_PERCENT: float = float(os.getenv("MAX_SLIPPAGE_PERCENT", "10.0"))
MAX_TRADE_USD: float = float(os.getenv("MAX_TRADE_USD", "100.0"))
MIN_TRADE_USD: float = float(os.getenv("MIN_TRADE_USD", "1.0"))

# =============================================================================
# Operation Mode
# =============================================================================
DRY_RUN: bool = os.getenv("DRY_RUN", "true").lower() in ("true", "1", "yes")

# =============================================================================
# Storage
# =============================================================================
DATABASE_PATH: str = os.getenv("DATABASE_PATH", "copied_trades.db")

# =============================================================================
# Logging
# =============================================================================
LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO")

# =============================================================================
# Hourly Trading Bot Configuration
# =============================================================================
# Position sizing
HOURLY_POSITION_SIZE_USD: float = float(os.getenv("HOURLY_POSITION_SIZE_USD", "100"))
HOURLY_MAX_EXPOSURE_PER_MARKET: float = float(os.getenv("HOURLY_MAX_EXPOSURE_PER_MARKET", "200"))
HOURLY_MAX_TOTAL_EXPOSURE: float = float(os.getenv("HOURLY_MAX_TOTAL_EXPOSURE", "500"))

# Trading thresholds
HOURLY_PROFIT_TAKE_PCT: float = float(os.getenv("HOURLY_PROFIT_TAKE_PCT", "15"))
HOURLY_HEDGE_TRIGGER_PCT: float = float(os.getenv("HOURLY_HEDGE_TRIGGER_PCT", "-10"))

# Timing
HOURLY_RE_ENTRY_COOLDOWN_SEC: int = int(os.getenv("HOURLY_RE_ENTRY_COOLDOWN_SEC", "30"))
HOURLY_POLL_INTERVAL_SEC: int = int(os.getenv("HOURLY_POLL_INTERVAL_SEC", "15"))

# Entry filters
HOURLY_MAX_SPREAD_TO_ENTER: float = float(os.getenv("HOURLY_MAX_SPREAD_TO_ENTER", "0.03"))
HOURLY_MIN_MINUTES_TO_TRADE: int = int(os.getenv("HOURLY_MIN_MINUTES_TO_TRADE", "5"))


def validate_config() -> list[str]:
    """
    Validate required configuration.
    Returns list of error messages (empty if valid).
    """
    errors = []
    
    if not TARGET_WALLET:
        errors.append("TARGET_WALLET is required")
    elif not TARGET_WALLET.startswith("0x"):
        errors.append("TARGET_WALLET must be a valid address starting with 0x")
    
    if not PRIVATE_KEY:
        errors.append("PRIVATE_KEY is required")
    elif not PRIVATE_KEY.startswith("0x"):
        errors.append("PRIVATE_KEY must start with 0x")
    
    if MAX_SLIPPAGE_PERCENT <= 0 or MAX_SLIPPAGE_PERCENT > 100:
        errors.append("MAX_SLIPPAGE_PERCENT must be between 0 and 100")
    
    if MAX_TRADE_USD <= 0:
        errors.append("MAX_TRADE_USD must be positive")
    
    if MIN_TRADE_USD <= 0:
        errors.append("MIN_TRADE_USD must be positive")
    
    if MIN_TRADE_USD > MAX_TRADE_USD:
        errors.append("MIN_TRADE_USD cannot exceed MAX_TRADE_USD")
    
    return errors
