"""
Hourly Crypto Trading Bot - Active Scalping Strategy

Trades BTC/ETH/SOL hourly Up/Down markets on Polymarket.
Uses Binance price data to inform decisions, takes profit on winning trades,
hedges losers, and re-enters for multiple round-trips per hour.
"""

import asyncio
import httpx
import json
import logging
import time
from datetime import datetime, timezone, timedelta
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional
from pathlib import Path
from zoneinfo import ZoneInfo

from web3 import Web3
from eth_account import Account

# =============================================================================
# Patch py-clob-client to use browser-like headers and optional proxy
# Must be done BEFORE importing ClobClient
# =============================================================================
from dotenv import load_dotenv
load_dotenv()  # Load .env FIRST so PROXY_URL is available

from py_clob_client.http_helpers import helpers as _clob_helpers
import httpx as _httpx

# 1. Patch headers to look like a browser
_original_overload_headers = _clob_helpers.overloadHeaders

def _patched_overload_headers(method, headers):
    headers = _original_overload_headers(method, headers)
    # Use browser-like User-Agent instead of "py_clob_client"
    headers['User-Agent'] = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    return headers

_clob_helpers.overloadHeaders = _patched_overload_headers

# 2. Configure proxy if PROXY_URL is set
import os as _os
_proxy_url = _os.getenv("PROXY_URL", "")
if _proxy_url:
    # Replace the global http client with one that uses the proxy
    _clob_helpers._http_client = _httpx.Client(
        proxy=_proxy_url,
        timeout=30.0,
    )
    print(f"[PROXY] CLOB API requests will use proxy: {_proxy_url.split('@')[-1] if '@' in _proxy_url else _proxy_url}")
# =============================================================================

import config
from api.clob_client import ClobClient
from storage.state import record_dry_run_trade

# Eastern Time zone (handles DST automatically)
ET_TZ = ZoneInfo("America/New_York")

# Configure logging
logging.basicConfig(
    level=getattr(logging, config.LOG_LEVEL, logging.INFO),
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# =============================================================================
# Configuration Parameters (from config.py after we update it)
# =============================================================================
POSITION_SIZE_USD = float(getattr(config, 'HOURLY_POSITION_SIZE_USD', 100))
MAX_EXPOSURE_PER_MARKET = float(getattr(config, 'HOURLY_MAX_EXPOSURE_PER_MARKET', 200))
MAX_TOTAL_EXPOSURE = float(getattr(config, 'HOURLY_MAX_TOTAL_EXPOSURE', 500))
PROFIT_TAKE_PCT = float(getattr(config, 'HOURLY_PROFIT_TAKE_PCT', 15))
RE_ENTRY_COOLDOWN_SEC = int(getattr(config, 'HOURLY_RE_ENTRY_COOLDOWN_SEC', 30))
POLL_INTERVAL_SEC = int(getattr(config, 'HOURLY_POLL_INTERVAL_SEC', 15))
MAX_SPREAD_TO_ENTER = float(getattr(config, 'HOURLY_MAX_SPREAD_TO_ENTER', 0.03))
MIN_MINUTES_TO_TRADE = int(getattr(config, 'HOURLY_MIN_MINUTES_TO_TRADE', 5))
CLOSE_POSITIONS_MINUTES = int(getattr(config, 'HOURLY_CLOSE_POSITIONS_MINUTES', 3))
WAIT_MINUTES = int(getattr(config, 'HOURLY_WAIT_MINUTES', 3))
STOP_LOSS_PCT = float(getattr(config, 'HOURLY_STOP_LOSS_PCT', -40))

# Confidence-based sizing
TREND_THRESHOLD = float(getattr(config, 'HOURLY_TREND_THRESHOLD', 0.25))
CONFIDENCE_LOW = float(getattr(config, 'HOURLY_CONFIDENCE_LOW', 0.3))
CONFIDENCE_HIGH = float(getattr(config, 'HOURLY_CONFIDENCE_HIGH', 0.6))
MAX_SIZE_MULTIPLIER = float(getattr(config, 'HOURLY_MAX_SIZE_MULTIPLIER', 3.0))
MAX_SLIPPAGE_PCT = float(getattr(config, 'HOURLY_MAX_SLIPPAGE_PCT', 5.0))
DIRECTIONAL_COOLDOWN_SEC = int(getattr(config, 'HOURLY_DIRECTIONAL_COOLDOWN_SEC', 120))
MAX_SAME_SIDE_RETRIES = int(getattr(config, 'HOURLY_MAX_SAME_SIDE_RETRIES', 2))

# API URLs
BINANCE_API_URL = "https://api.binance.com/api/v3"
BINANCE_US_API_URL = "https://api.binance.us/api/v3"
COINGECKO_API_URL = "https://api.coingecko.com/api/v3"
GAMMA_API_URL = "https://gamma-api.polymarket.com"

# Asset mapping
ASSETS = {
    "BTC": {"binance": "BTCUSDT", "polymarket": "bitcoin", "coingecko": "bitcoin"},
    "ETH": {"binance": "ETHUSDT", "polymarket": "ethereum", "coingecko": "ethereum"},
    "SOL": {"binance": "SOLUSDT", "polymarket": "solana", "coingecko": "solana"},
}

# =============================================================================
# Redemption Configuration (for resolved positions)
# =============================================================================
POLYGON_RPC_URL = "https://polygon-rpc.com"
USDC_E_ADDRESS = Web3.to_checksum_address('0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174')
CONDITIONAL_TOKENS_ADDRESS = Web3.to_checksum_address('0x4D97DCd97eC945f40cF65F87097ACe5EA0476045')
DATA_API_URL = "https://data-api.polymarket.com"

# Minimum value to attempt redemption (skip dust)
MIN_REDEMPTION_VALUE = 0.01  # $0.01

# CTF ABI for redeemPositions
CTF_REDEEM_ABI = [
    {
        "inputs": [
            {"name": "collateralToken", "type": "address"},
            {"name": "parentCollectionId", "type": "bytes32"},
            {"name": "conditionId", "type": "bytes32"},
            {"name": "indexSets", "type": "uint256[]"}
        ],
        "name": "redeemPositions",
        "outputs": [],
        "stateMutability": "nonpayable",
        "type": "function"
    }
]

USDC_BALANCE_ABI = [
    {
        'constant': True,
        'inputs': [{'name': '_owner', 'type': 'address'}],
        'name': 'balanceOf',
        'outputs': [{'name': 'balance', 'type': 'uint256'}],
        'type': 'function'
    }
]


# =============================================================================
# Enums and Data Classes
# =============================================================================

class PositionState(Enum):
    NONE = "NONE"
    LONG_DOWN = "LONG_DOWN"
    LONG_UP = "LONG_UP"


class Action(Enum):
    HOLD = "HOLD"
    BUY_DOWN = "BUY_DOWN"
    BUY_UP = "BUY_UP"
    SELL_DOWN = "SELL_DOWN"
    SELL_UP = "SELL_UP"


@dataclass
class Position:
    """Represents a position in one side of a market."""
    side: str  # "UP" or "DOWN"
    entry_price: float
    size: float  # in shares
    cost: float  # USD spent
    entry_time: datetime


@dataclass
class MarketPosition:
    """Tracks positions for a single market."""
    up_position: Optional[Position] = None
    down_position: Optional[Position] = None
    last_exit_time: Optional[datetime] = None
    last_exit_side: Optional[str] = None  # "UP" or "DOWN"
    same_side_exit_count: int = 0  # consecutive exits on same side
    peak_pnl_pct: float = 0.0  # track peak P&L for trailing stop

    def get_state(self) -> PositionState:
        if self.up_position is not None:
            return PositionState.LONG_UP
        elif self.down_position is not None:
            return PositionState.LONG_DOWN
        else:
            return PositionState.NONE

    def get_total_exposure(self) -> float:
        total = 0.0
        if self.up_position:
            total += self.up_position.cost
        if self.down_position:
            total += self.down_position.cost
        return total


@dataclass
class TradeRecord:
    """Record of a single trade."""
    time: str
    market: str
    action: str
    price: float
    size: float
    pnl: Optional[float] = None


@dataclass
class HourlySession:
    """Tracks all activity for one hour."""
    hour_start: datetime
    asset: str
    trades: list[TradeRecord] = field(default_factory=list)
    total_pnl: float = 0.0


# =============================================================================
# PriceFetcher - Real-time price data with multiple source fallback
# =============================================================================

class PriceFetcher:
    """
    Fetches real-time price data with fallback sources.
    
    Tries in order:
    1. Binance.com (global)
    2. Binance.us (US-accessible)
    3. CoinGecko (universal fallback)
    """
    
    def __init__(self, http: httpx.AsyncClient):
        self.http = http
        self._hourly_opens: dict[str, float] = {}
        self._last_hour_fetched: dict[str, int] = {}
        self._working_source: Optional[str] = None  # Cache which source works
    
    async def _try_binance_price(self, symbol: str, base_url: str) -> Optional[float]:
        """Try to get price from a Binance API."""
        try:
            url = f"{base_url}/ticker/price"
            params = {"symbol": symbol}
            response = await self.http.get(url, params=params, timeout=5.0)
            if response.status_code == 200:
                data = response.json()
                return float(data["price"])
        except Exception:
            pass
        return None
    
    async def _try_binance_kline(self, symbol: str, base_url: str) -> Optional[float]:
        """Try to get hourly open from a Binance API."""
        try:
            url = f"{base_url}/klines"
            params = {"symbol": symbol, "interval": "1h", "limit": 1}
            response = await self.http.get(url, params=params, timeout=5.0)
            if response.status_code == 200:
                data = response.json()
                if data:
                    return float(data[0][1])  # Open price
        except Exception:
            pass
        return None
    
    async def _try_coingecko_price(self, coin_id: str) -> Optional[float]:
        """Try to get price from CoinGecko."""
        try:
            url = f"{COINGECKO_API_URL}/simple/price"
            params = {"ids": coin_id, "vs_currencies": "usd"}
            response = await self.http.get(url, params=params, timeout=5.0)
            if response.status_code == 200:
                data = response.json()
                if coin_id in data and "usd" in data[coin_id]:
                    return float(data[coin_id]["usd"])
        except Exception:
            pass
        return None
    
    async def get_current_price(self, symbol: str, coingecko_id: str = None) -> float:
        """
        Get current price with fallback sources.
        
        Args:
            symbol: Binance symbol (e.g., BTCUSDT)
            coingecko_id: CoinGecko coin ID (e.g., bitcoin)
        """
        # Try Binance.com first
        price = await self._try_binance_price(symbol, BINANCE_API_URL)
        if price is not None:
            self._working_source = "binance.com"
            return price
        
        # Try Binance.us
        price = await self._try_binance_price(symbol, BINANCE_US_API_URL)
        if price is not None:
            self._working_source = "binance.us"
            return price
        
        # Fall back to CoinGecko
        if coingecko_id:
            price = await self._try_coingecko_price(coingecko_id)
            if price is not None:
                self._working_source = "coingecko"
                return price
        
        raise ValueError(f"Could not fetch price for {symbol} from any source")
    
    async def get_hourly_open(self, symbol: str, coingecko_id: str = None) -> float:
        """
        Get the open price for the current hourly candle.
        
        Note: CoinGecko doesn't provide hourly candles easily, so we use
        the price at the start of the hour (cached) as approximation.
        """
        current_hour = datetime.now(timezone.utc).hour
        
        # Check cache
        cache_key = f"{symbol}_{current_hour}"
        if cache_key in self._hourly_opens:
            return self._hourly_opens[cache_key]
        
        # Try Binance.com
        open_price = await self._try_binance_kline(symbol, BINANCE_API_URL)
        if open_price is not None:
            self._hourly_opens[cache_key] = open_price
            return open_price
        
        # Try Binance.us
        open_price = await self._try_binance_kline(symbol, BINANCE_US_API_URL)
        if open_price is not None:
            self._hourly_opens[cache_key] = open_price
            return open_price
        
        # CoinGecko fallback: use current price as "open" if we don't have it cached
        # This is less accurate but better than failing
        if coingecko_id:
            logger.warning(f"Using CoinGecko current price as hourly open for {symbol} (less accurate)")
            price = await self._try_coingecko_price(coingecko_id)
            if price is not None:
                self._hourly_opens[cache_key] = price
                return price
        
        raise ValueError(f"Could not fetch hourly open for {symbol} from any source")
    
    async def get_price_change_pct(self, symbol: str, coingecko_id: str = None) -> float:
        """Get percentage change from hourly open."""
        open_price = await self.get_hourly_open(symbol, coingecko_id)
        current_price = await self.get_current_price(symbol, coingecko_id)
        
        return ((current_price - open_price) / open_price) * 100
    
    async def get_price_data(self, symbol: str, coingecko_id: str = None) -> dict:
        """Get all price data for a symbol."""
        open_price = await self.get_hourly_open(symbol, coingecko_id)
        current_price = await self.get_current_price(symbol, coingecko_id)
        pct_change = ((current_price - open_price) / open_price) * 100
        
        return {
            "symbol": symbol,
            "open": open_price,
            "current": current_price,
            "pct_change": pct_change,
            "source": self._working_source,
        }


# =============================================================================
# PolymarketFetcher - Market data from Polymarket
# =============================================================================

class PolymarketFetcher:
    """Fetches market data from Polymarket Gamma API."""
    
    def __init__(self, http: httpx.AsyncClient):
        self.http = http
    
    def _generate_slug(self, asset: str, hour_time: datetime) -> str:
        """Generate event slug for hourly market."""
        months = [
            "january", "february", "march", "april", "may", "june",
            "july", "august", "september", "october", "november", "december"
        ]
        
        month_name = months[hour_time.month - 1]
        day = hour_time.day
        hour = hour_time.hour
        
        if hour == 0:
            hour_str = "12am"
        elif hour < 12:
            hour_str = f"{hour}am"
        elif hour == 12:
            hour_str = "12pm"
        else:
            hour_str = f"{hour - 12}pm"
        
        return f"{asset}-up-or-down-{month_name}-{day}-{hour_str}-et"
    
    async def get_market_data(self, asset_name: str) -> Optional[dict]:
        """
        Get current hourly market data for an asset.
        
        Returns dict with: slug, condition_id, best_bid, best_ask, 
                          up_price, down_price, token_ids, etc.
        """
        # Calculate current ET hour (handles DST automatically)
        utc_now = datetime.now(timezone.utc)
        et_now = utc_now.astimezone(ET_TZ)
        et_hour = et_now.replace(minute=0, second=0, microsecond=0)
        
        slug = self._generate_slug(asset_name, et_hour)
        
        try:
            url = f"{GAMMA_API_URL}/events/slug/{slug}"
            response = await self.http.get(url)
            
            if response.status_code != 200:
                logger.warning(f"Market not found: {slug}")
                return None
            
            event = response.json()
            markets = event.get("markets", [])
            
            if not markets:
                return None
            
            market = markets[0]
            
            # Parse token IDs and outcomes
            token_ids_str = market.get("clobTokenIds", "[]")
            token_ids = json.loads(token_ids_str) if isinstance(token_ids_str, str) else token_ids_str
            
            outcomes_str = market.get("outcomes", "[]")
            outcomes = json.loads(outcomes_str) if isinstance(outcomes_str, str) else outcomes_str
            
            prices_str = market.get("outcomePrices", "[]")
            prices = json.loads(prices_str) if isinstance(prices_str, str) else prices_str
            prices = [float(p) for p in prices]
            
            # Find Up and Down indices
            up_idx = outcomes.index("Up") if "Up" in outcomes else 0
            down_idx = outcomes.index("Down") if "Down" in outcomes else 1
            
            # Parse end time
            end_str = market.get("endDate", "")
            end_time = None
            if end_str:
                if end_str.endswith("Z"):
                    end_str = end_str[:-1] + "+00:00"
                try:
                    end_time = datetime.fromisoformat(end_str)
                except:
                    pass
            
            return {
                "slug": slug,
                "condition_id": market.get("conditionId", ""),
                "best_bid": float(market.get("bestBid", 0) or 0),
                "best_ask": float(market.get("bestAsk", 0) or 0),
                "spread": float(market.get("spread", 0) or 0),
                "up_price": prices[up_idx] if up_idx < len(prices) else 0.5,
                "down_price": prices[down_idx] if down_idx < len(prices) else 0.5,
                "up_token_id": token_ids[up_idx] if up_idx < len(token_ids) else "",
                "down_token_id": token_ids[down_idx] if down_idx < len(token_ids) else "",
                "end_time": end_time,
                "volume": float(market.get("volume", 0)),
                "liquidity": float(market.get("liquidity", 0)),
            }
            
        except Exception as e:
            logger.error(f"Error fetching market data for {asset_name}: {e}")
            return None


# =============================================================================
# PositionManager - Track positions and P&L
# =============================================================================

class PositionManager:
    """Manages positions across all markets."""
    
    def __init__(self):
        self.positions: dict[str, MarketPosition] = {}  # asset -> MarketPosition
    
    def get_state(self, asset: str) -> PositionState:
        """Get position state for an asset."""
        if asset not in self.positions:
            return PositionState.NONE
        return self.positions[asset].get_state()
    
    def get_position(self, asset: str) -> MarketPosition:
        """Get or create MarketPosition for an asset."""
        if asset not in self.positions:
            self.positions[asset] = MarketPosition()
        return self.positions[asset]
    
    def open_position(self, asset: str, side: str, price: float, size_usd: float) -> Position:
        """
        Open a new position.

        Args:
            asset: Asset symbol (BTC, ETH, SOL)
            side: "UP" or "DOWN"
            price: Entry price per share
            size_usd: USD amount to spend

        Returns:
            The created Position
        """
        shares = size_usd / price
        position = Position(
            side=side,
            entry_price=price,
            size=shares,
            cost=size_usd,
            entry_time=datetime.now(timezone.utc),
        )
        
        market_pos = self.get_position(asset)
        
        if side == "UP":
            market_pos.up_position = position
        else:
            market_pos.down_position = position
        
        logger.info(f"Opened {side} position for {asset}: {shares:.2f} shares @ ${price:.4f} (${size_usd:.2f})")
        return position
    
    def close_position(self, asset: str, side: str, exit_price: float) -> float:
        """
        Close a position and return P&L.
        
        Args:
            asset: Asset symbol
            side: "UP" or "DOWN"
            exit_price: Exit price per share
        
        Returns:
            P&L in USD
        """
        market_pos = self.get_position(asset)
        
        if side == "UP":
            position = market_pos.up_position
            market_pos.up_position = None
        else:
            position = market_pos.down_position
            market_pos.down_position = None
        
        if position is None:
            logger.warning(f"No {side} position to close for {asset}")
            return 0.0

        # Calculate P&L
        exit_value = position.size * exit_price
        pnl = exit_value - position.cost

        now = datetime.now(timezone.utc)
        # Track directional cooldown
        if market_pos.last_exit_side == side:
            market_pos.same_side_exit_count += 1
        else:
            market_pos.same_side_exit_count = 1
        market_pos.last_exit_time = now
        market_pos.last_exit_side = side
        market_pos.peak_pnl_pct = 0.0  # Reset trailing stop tracker

        logger.info(f"Closed {side} position for {asset}: {position.size:.2f} shares @ ${exit_price:.4f}, P&L: ${pnl:.2f}")
        return pnl
    
    def reduce_position(self, asset: str, side: str, shares_sold: float, exit_price: float) -> float:
        """
        Partially close a position (used when FAK sells only some shares).
        
        Args:
            asset: Asset symbol
            side: "UP" or "DOWN"
            shares_sold: Number of shares sold
            exit_price: Exit price per share
        
        Returns:
            P&L from the partial sale
        """
        market_pos = self.get_position(asset)
        
        if side == "UP":
            position = market_pos.up_position
        else:
            position = market_pos.down_position
        
        if position is None:
            logger.warning(f"No {side} position to reduce for {asset}")
            return 0.0
        
        shares_sold = min(shares_sold, position.size)
        cost_allocated = position.cost * (shares_sold / position.size)
        exit_value = shares_sold * exit_price
        pnl = exit_value - cost_allocated
        
        position.size -= shares_sold
        position.cost -= cost_allocated
        
        if position.size <= 0:
            if side == "UP":
                market_pos.up_position = None
            else:
                market_pos.down_position = None
            market_pos.last_exit_time = datetime.now(timezone.utc)
            logger.info(f"Closed {side} position for {asset}: {shares_sold:.2f} shares @ ${exit_price:.4f}, P&L: ${pnl:.2f} (full close via partial fills)")
        else:
            logger.info(f"Partial close {side} for {asset}: {shares_sold:.2f} shares @ ${exit_price:.4f}, P&L: ${pnl:.2f} ({position.size:.2f} shares remaining)")
        
        return pnl
    
    def get_position_pnl_pct(self, asset: str, side: str, current_price: float) -> float:
        """Get current P&L percentage for a position."""
        market_pos = self.get_position(asset)
        
        if side == "UP":
            position = market_pos.up_position
        else:
            position = market_pos.down_position
        
        if position is None:
            return 0.0
        
        current_value = position.size * current_price
        pnl_pct = ((current_value - position.cost) / position.cost) * 100
        return pnl_pct
    
    def get_total_exposure(self) -> float:
        """Get total USD exposure across all markets."""
        total = 0.0
        for market_pos in self.positions.values():
            total += market_pos.get_total_exposure()
        return total
    
    def can_cooldown_trade(self, asset: str) -> bool:
        """Check if cooldown period has passed since last exit."""
        market_pos = self.get_position(asset)

        if market_pos.last_exit_time is None:
            return True

        elapsed = (datetime.now(timezone.utc) - market_pos.last_exit_time).total_seconds()
        return elapsed >= RE_ENTRY_COOLDOWN_SEC

    def can_enter_side(self, asset: str, side: str) -> bool:
        """
        Check if we can enter a specific side, applying directional cooldown.
        Prevents whipsaw by enforcing longer cooldown for same-side re-entry
        and limiting consecutive same-side entries.
        """
        market_pos = self.get_position(asset)

        if market_pos.last_exit_side is None:
            return True

        # If re-entering the SAME side we just exited, enforce longer cooldown
        if market_pos.last_exit_side == side:
            # Block if exceeded max same-side retries
            if market_pos.same_side_exit_count >= MAX_SAME_SIDE_RETRIES:
                logger.info(
                    f"Blocked {side} re-entry for {asset}: {market_pos.same_side_exit_count} "
                    f"consecutive {side} exits (max {MAX_SAME_SIDE_RETRIES})"
                )
                return False

            # Enforce directional cooldown (longer than normal cooldown)
            if market_pos.last_exit_time:
                elapsed = (datetime.now(timezone.utc) - market_pos.last_exit_time).total_seconds()
                if elapsed < DIRECTIONAL_COOLDOWN_SEC:
                    logger.debug(
                        f"Directional cooldown for {asset} {side}: {elapsed:.0f}s / {DIRECTIONAL_COOLDOWN_SEC}s"
                    )
                    return False

        return True

    def update_peak_pnl(self, asset: str, current_pnl_pct: float):
        """Track peak P&L for trailing stop."""
        market_pos = self.get_position(asset)
        if current_pnl_pct > market_pos.peak_pnl_pct:
            market_pos.peak_pnl_pct = current_pnl_pct

    def clear_all(self):
        """Clear all positions (for new hour)."""
        self.positions.clear()


# =============================================================================
# TradingStateMachine - Core decision logic
# =============================================================================

class TradingStateMachine:
    """Makes trading decisions based on current state and market conditions."""

    # Thresholds for lag-based trading
    MAX_ENTRY_PRICE = 0.60  # Don't buy if side is already this expensive (lag closed)
    PROFIT_TARGET = 20.0    # Base profit target (scaled by entry price)

    # Trailing stop parameters
    TRAILING_STOP_ACTIVATE_PCT = 10.0  # Activate trailing stop after 10% profit
    TRAILING_STOP_DISTANCE_PCT = 5.0   # Exit if drops 5% from peak

    def get_dynamic_profit_target(self, entry_price: float, minutes_remaining: int = 30) -> float:
        """
        Scale profit target inversely with entry price, adjusted for time.

        Better entries (lower price) get higher targets since there's more upside.
        Early in hour: widen target (more time). Late: narrow target (take what you can).
        """
        if entry_price < 0.30:
            base_target = self.PROFIT_TARGET * 2.5  # 50%
        elif entry_price < 0.40:
            base_target = self.PROFIT_TARGET * 2.0  # 40%
        elif entry_price < 0.50:
            base_target = self.PROFIT_TARGET * 1.5  # 30%
        else:
            base_target = self.PROFIT_TARGET  # 20%

        # Time-based adjustment
        if minutes_remaining >= 40:
            return base_target * 1.2  # 20% wider early
        elif minutes_remaining <= 10:
            return max(base_target * 0.5, 5.0)  # 50% narrower late, minimum 5%
        return base_target

    def calculate_confidence(
        self,
        binance_pct: float,
        polymarket_price: float,
        liquidity: float,
        minutes_remaining: int,
        trend_confidence: float,
    ) -> float:
        """
        Calculate a confidence score from 0.0 to 1.0 for the trade signal.

        Factors:
        1. Magnitude of Binance move (bigger = higher confidence)
        2. Polymarket price discount (cheaper entry = more room)
        3. Order book liquidity (more liquidity = less risk)
        4. Time remaining in hour (earlier = more time for thesis)
        5. Trend consistency (MA aligned with raw reading = more stable)
        """
        confidence = 0.0

        # Factor 1: Binance move magnitude (0-0.3)
        move_abs = abs(binance_pct)
        if move_abs >= 1.0:
            confidence += 0.30
        elif move_abs >= 0.5:
            confidence += 0.20
        elif move_abs >= 0.25:
            confidence += 0.10

        # Factor 2: Polymarket discount (0-0.3)
        if polymarket_price <= 0.35:
            confidence += 0.30
        elif polymarket_price <= 0.45:
            confidence += 0.20
        elif polymarket_price <= 0.55:
            confidence += 0.10

        # Factor 3: Liquidity (0-0.15)
        if liquidity >= 500:
            confidence += 0.15
        elif liquidity >= 200:
            confidence += 0.10
        elif liquidity >= 50:
            confidence += 0.05

        # Factor 4: Time remaining (0-0.15)
        if minutes_remaining >= 45:
            confidence += 0.15
        elif minutes_remaining >= 30:
            confidence += 0.10
        elif minutes_remaining >= 15:
            confidence += 0.05

        # Factor 5: Trend consistency (0-0.10)
        if (binance_pct > 0 and trend_confidence > 0) or (binance_pct < 0 and trend_confidence < 0):
            confidence += 0.10

        return min(confidence, 1.0)

    def decide(
        self,
        state: PositionState,
        binance_pct: float,
        position_pnl_pct: float,
        minutes_remaining: int,
        spread: float,
        can_trade_cooldown: bool,
        is_initial_entry: bool = False,
        down_price: float = 0.5,
        entry_price: float = 0.5,
        asset: str = None,
        trend_confidence: float = None,
        peak_pnl_pct: float = 0.0,
        can_enter_side_up: bool = True,
        can_enter_side_down: bool = True,
    ) -> Action:
        """
        Decide what action to take.

        Simplified strategy (no hedging, no lottery):
        1. Enter when Binance moves but Polymarket hasn't caught up (lag)
        2. Exit on profit target, trailing stop, trend reversal, or stop loss
        3. Re-enter opposite side if trend reverses
        """
        up_price = 1.0 - down_price
        trend_threshold = TREND_THRESHOLD

        # Use trend confidence (moving avg) if provided, otherwise raw binance_pct
        effective_trend = trend_confidence if trend_confidence is not None else binance_pct

        # Calculate dynamic profit target
        profit_target = self.get_dynamic_profit_target(entry_price, minutes_remaining)

        # Don't open new positions in last few minutes
        if minutes_remaining < MIN_MINUTES_TO_TRADE:
            if state == PositionState.NONE:
                return Action.HOLD

        # Force close all positions in final minutes
        if minutes_remaining <= CLOSE_POSITIONS_MINUTES:
            if state == PositionState.LONG_DOWN:
                logger.info(f"Hour ending - force closing DOWN for {asset}")
                return Action.SELL_DOWN
            elif state == PositionState.LONG_UP:
                logger.info(f"Hour ending - force closing UP for {asset}")
                return Action.SELL_UP

        # Check spread - don't enter if too wide
        if spread > MAX_SPREAD_TO_ENTER and state == PositionState.NONE:
            logger.debug(f"Spread {spread:.4f} too wide, holding")
            return Action.HOLD

        # Wait period at start of hour
        minutes_elapsed = 60 - minutes_remaining
        if minutes_elapsed < WAIT_MINUTES and state == PositionState.NONE:
            logger.debug(f"Wait period: {minutes_elapsed}m elapsed, need {WAIT_MINUTES}m")
            return Action.HOLD

        # === NO POSITION - Look for entry ===
        if state == PositionState.NONE:
            if not can_trade_cooldown:
                return Action.HOLD

            logger.info(f"Entry check: DOWN={down_price:.4f}, UP={up_price:.4f}, Binance={binance_pct:+.2f}%")

            # Hard cap: never buy either side above 0.70
            if down_price > 0.70 and up_price > 0.70:
                logger.info(f"Both sides expensive (DOWN={down_price:.2f}, UP={up_price:.2f}), skipping")
                return Action.HOLD

            # Spread-aware entry: expected move must exceed 2x spread cost
            expected_move = abs(binance_pct) * 0.01
            if expected_move < spread * 2 and not is_initial_entry:
                logger.debug(f"Expected move ({expected_move:.4f}) < 2x spread ({spread*2:.4f}), skipping")
                return Action.HOLD

            # Initial entry: use Binance direction with lag detection
            if is_initial_entry:
                if binance_pct < -0.15 and down_price <= 0.55 and can_enter_side_down:
                    logger.info(f"Initial entry: Binance down {binance_pct:.2f}%, DOWN at {down_price:.2f}")
                    return Action.BUY_DOWN
                elif binance_pct > 0.15 and up_price <= 0.55 and can_enter_side_up:
                    logger.info(f"Initial entry: Binance up {binance_pct:.2f}%, UP at {up_price:.2f}")
                    return Action.BUY_UP
                # No significant trend - buy cheaper side
                elif down_price <= 0.50 and can_enter_side_down:
                    logger.info(f"Initial entry: No trend, buying cheaper DOWN at {down_price:.2f}")
                    return Action.BUY_DOWN
                elif can_enter_side_up:
                    logger.info(f"Initial entry: No trend, buying cheaper UP at {up_price:.2f}")
                    return Action.BUY_UP
                return Action.HOLD

            # Re-entry: follow trend ONLY if lag exists
            if binance_pct < -trend_threshold and can_enter_side_down:
                if down_price <= self.MAX_ENTRY_PRICE:
                    logger.info(f"Lag detected: Binance {binance_pct:.2f}% but DOWN only {down_price:.2f}")
                    return Action.BUY_DOWN
                else:
                    logger.info(f"No lag: Binance {binance_pct:.2f}%, DOWN already at {down_price:.2f}")
                    return Action.HOLD
            elif binance_pct > trend_threshold and can_enter_side_up:
                if up_price <= self.MAX_ENTRY_PRICE:
                    logger.info(f"Lag detected: Binance {binance_pct:.2f}% but UP only {up_price:.2f}")
                    return Action.BUY_UP
                else:
                    logger.info(f"No lag: Binance {binance_pct:.2f}%, UP already at {up_price:.2f}")
                    return Action.HOLD
            else:
                logger.debug(f"No significant trend (Binance {binance_pct:.2f}%), holding")
                return Action.HOLD

        # === HOLDING DOWN ===
        elif state == PositionState.LONG_DOWN:
            # Hard stop-loss
            if position_pnl_pct <= STOP_LOSS_PCT:
                logger.info(f"STOP LOSS on DOWN: {position_pnl_pct:.1f}% (limit: {STOP_LOSS_PCT}%)")
                return Action.SELL_DOWN

            # Profit target
            if position_pnl_pct >= profit_target:
                logger.info(f"Taking profit on DOWN: {position_pnl_pct:.1f}% (target: {profit_target:.1f}%)")
                return Action.SELL_DOWN

            # Trailing stop: if was up 10%+ and dropped 5% from peak
            if peak_pnl_pct >= self.TRAILING_STOP_ACTIVATE_PCT:
                if position_pnl_pct < peak_pnl_pct - self.TRAILING_STOP_DISTANCE_PCT:
                    logger.info(
                        f"TRAILING STOP on DOWN: peak was {peak_pnl_pct:.1f}%, "
                        f"now {position_pnl_pct:.1f}%"
                    )
                    return Action.SELL_DOWN

            # Trend reversal exit (profitable or strong reversal)
            if effective_trend > trend_threshold:
                if position_pnl_pct > 0:
                    logger.info(f"Trend reversed to +{effective_trend:.2f}%, exiting DOWN with {position_pnl_pct:.1f}%")
                    return Action.SELL_DOWN
                # Even at a loss, exit if reversal is strong (> 2x threshold)
                if effective_trend > trend_threshold * 2:
                    logger.info(f"Strong reversal +{effective_trend:.2f}%, cutting DOWN at {position_pnl_pct:.1f}%")
                    return Action.SELL_DOWN

            return Action.HOLD

        # === HOLDING UP ===
        elif state == PositionState.LONG_UP:
            # Hard stop-loss
            if position_pnl_pct <= STOP_LOSS_PCT:
                logger.info(f"STOP LOSS on UP: {position_pnl_pct:.1f}% (limit: {STOP_LOSS_PCT}%)")
                return Action.SELL_UP

            # Profit target
            if position_pnl_pct >= profit_target:
                logger.info(f"Taking profit on UP: {position_pnl_pct:.1f}% (target: {profit_target:.1f}%)")
                return Action.SELL_UP

            # Trailing stop
            if peak_pnl_pct >= self.TRAILING_STOP_ACTIVATE_PCT:
                if position_pnl_pct < peak_pnl_pct - self.TRAILING_STOP_DISTANCE_PCT:
                    logger.info(
                        f"TRAILING STOP on UP: peak was {peak_pnl_pct:.1f}%, "
                        f"now {position_pnl_pct:.1f}%"
                    )
                    return Action.SELL_UP

            # Trend reversal exit
            if effective_trend < -trend_threshold:
                if position_pnl_pct > 0:
                    logger.info(f"Trend reversed to {effective_trend:.2f}%, exiting UP with {position_pnl_pct:.1f}%")
                    return Action.SELL_UP
                if effective_trend < -trend_threshold * 2:
                    logger.info(f"Strong reversal {effective_trend:.2f}%, cutting UP at {position_pnl_pct:.1f}%")
                    return Action.SELL_UP

            return Action.HOLD

        return Action.HOLD


# =============================================================================
# TradeLogger - Record trades for analysis
# =============================================================================

class TradeLogger:
    """Logs all trades to JSON file for analysis."""
    
    def __init__(self, log_dir: str = "trade_logs"):
        self.log_dir = Path(log_dir)
        self.log_dir.mkdir(exist_ok=True)
        self.sessions: dict[str, HourlySession] = {}  # asset -> current session
    
    def start_session(self, asset: str, hour_start: datetime):
        """Start a new hourly session."""
        self.sessions[asset] = HourlySession(
            hour_start=hour_start,
            asset=asset,
        )
    
    def log_trade(
        self,
        asset: str,
        action: Action,
        price: float,
        size: float,
        pnl: Optional[float] = None,
    ):
        """Log a trade."""
        if asset not in self.sessions:
            self.start_session(asset, datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0))
        
        trade = TradeRecord(
            time=datetime.now(timezone.utc).strftime("%H:%M:%S"),
            market=asset,
            action=action.value,
            price=price,
            size=size,
            pnl=pnl,
        )
        
        self.sessions[asset].trades.append(trade)
        
        if pnl is not None:
            self.sessions[asset].total_pnl += pnl
    
    def save_session(self, asset: str):
        """Save session to JSON file."""
        if asset not in self.sessions:
            return
        
        session = self.sessions[asset]
        
        filename = f"{session.hour_start.strftime('%Y%m%d_%H%M')}_{asset}.json"
        filepath = self.log_dir / filename
        
        data = {
            "hour": session.hour_start.isoformat(),
            "asset": session.asset,
            "trades": [
                {
                    "time": t.time,
                    "action": t.action,
                    "price": t.price,
                    "size": t.size,
                    "pnl": t.pnl,
                }
                for t in session.trades
            ],
            "total_pnl": session.total_pnl,
            "num_trades": len(session.trades),
        }
        
        with open(filepath, "w") as f:
            json.dump(data, f, indent=2)
        
        logger.info(f"Saved session log to {filepath}")
    
    def get_session_pnl(self, asset: str) -> float:
        """Get total P&L for current session."""
        if asset not in self.sessions:
            return 0.0
        return self.sessions[asset].total_pnl


# =============================================================================
# HourlyTradingBot - Main bot class
# =============================================================================

class HourlyTradingBot:
    """Main trading bot that coordinates all components."""
    
    # Trend tracking settings
    TREND_HISTORY_SIZE = 4  # Number of readings to average for trend confidence
    
    def __init__(self, dry_run: bool = True):
        self.dry_run = dry_run
        self.http: Optional[httpx.AsyncClient] = None
        self.clob_client: Optional[ClobClient] = None
        self.price_fetcher: Optional[PriceFetcher] = None
        self.polymarket: Optional[PolymarketFetcher] = None
        self.position_manager = PositionManager()
        self.state_machine = TradingStateMachine()
        self.trade_logger = TradeLogger()
        self.current_hour: Optional[datetime] = None
        self.is_initial_entry: dict[str, bool] = {}  # asset -> whether initial entry done
        self._trend_history: dict[str, list[float]] = {}  # asset -> recent Binance readings
    
    def _update_trend_history(self, asset: str, binance_pct: float) -> float:
        """
        Update trend history and return moving average (trend confidence).
        
        Using a moving average smooths out noise and provides more stable
        signals for hedge exits and profit-taking decisions.
        """
        if asset not in self._trend_history:
            self._trend_history[asset] = []
        
        self._trend_history[asset].append(binance_pct)
        
        # Keep only recent readings
        if len(self._trend_history[asset]) > self.TREND_HISTORY_SIZE:
            self._trend_history[asset].pop(0)
        
        # Return moving average
        return sum(self._trend_history[asset]) / len(self._trend_history[asset])
    
    def _get_confidence_adjusted_size(self, confidence: float, base_size: float) -> float:
        """
        Map confidence score to position size multiplier (up to MAX_SIZE_MULTIPLIER).

        - confidence < CONFIDENCE_LOW: 1x (low confidence, minimum size)
        - confidence CONFIDENCE_LOW-CONFIDENCE_HIGH: 2x (medium confidence)
        - confidence > CONFIDENCE_HIGH: MAX_SIZE_MULTIPLIER (high confidence, max aggression)
        """
        if confidence >= CONFIDENCE_HIGH:
            return base_size * MAX_SIZE_MULTIPLIER
        elif confidence >= CONFIDENCE_LOW:
            return base_size * 2.0
        else:
            return base_size * 1.0
    
    async def _cancel_all_open_orders(self):
        """
        Cancel all open/unfilled orders on the CLOB to free locked USDC.
        
        GTC orders that didn't fill stay on the book locking collateral,
        even after markets resolve. This must be called at hour transitions
        and on startup to prevent 'not enough balance' errors.
        """
        if self.dry_run or not self.clob_client:
            return
        
        try:
            orders = await self.clob_client.get_open_orders()
            if not orders:
                logger.debug("No open orders to cancel")
                return
            
            logger.info(f"[CLEANUP] Found {len(orders)} open orders, cancelling all...")
            cancelled = 0
            for order in orders:
                order_id = order.get("id", "")
                if order_id:
                    success = await self.clob_client.cancel_order(order_id)
                    if success:
                        cancelled += 1
            
            logger.info(f"[CLEANUP] Cancelled {cancelled}/{len(orders)} stale orders")
        except Exception as e:
            logger.error(f"[CLEANUP] Error cancelling open orders: {e}")
    
    async def initialize(self):
        """Initialize all clients."""
        # Use proxy for Gamma API if configured (same proxy as CLOB)
        proxy_url = config.PROXY_URL if hasattr(config, 'PROXY_URL') and config.PROXY_URL else None
        self.http = httpx.AsyncClient(timeout=30.0, proxy=proxy_url)
        if proxy_url:
            logger.info(f"Using proxy for Gamma API: {proxy_url.split('@')[-1] if '@' in proxy_url else proxy_url}")
        self.price_fetcher = PriceFetcher(self.http)
        self.polymarket = PolymarketFetcher(self.http)
        
        if not self.dry_run:
            self.clob_client = ClobClient()
            await self.clob_client.initialize()
            # Cancel any stale orders left from previous runs
            await self._cancel_all_open_orders()
        
        logger.info(f"Bot initialized (dry_run={self.dry_run})")
    
    async def close(self):
        """Clean up resources."""
        if self.http:
            await self.http.aclose()
        if self.clob_client:
            await self.clob_client.close()
    
    def _get_current_et_hour(self) -> datetime:
        """Get the current hour in ET (handles DST automatically)."""
        utc_now = datetime.now(timezone.utc)
        et_now = utc_now.astimezone(ET_TZ)
        return et_now.replace(minute=0, second=0, microsecond=0)

    def _get_minutes_remaining(self) -> int:
        """Get minutes remaining in current hour."""
        utc_now = datetime.now(timezone.utc)
        et_now = utc_now.astimezone(ET_TZ)
        return 60 - et_now.minute
    
    async def _close_all_positions(self, asset: str, market_data: dict, reason: str = "hour end") -> float:
        """
        Close all open positions for an asset.
        
        This ensures positions are resolved before hour end rather than
        being left to market resolution (which would require tracking outcomes).
        
        Args:
            asset: Asset symbol
            market_data: Current market data with prices and token IDs
            reason: Reason for closing (for logging)
        
        Returns:
            Total P&L from closing positions
        """
        total_pnl = 0.0
        market_pos = self.position_manager.get_position(asset)
        
        # Close UP position if exists
        if market_pos.up_position:
            logger.info(f"Closing UP position for {asset} ({reason})")
            pnl = await self._execute_trade(asset, Action.SELL_UP, market_data)
            if pnl is not None:
                total_pnl += pnl
        
        # Close DOWN position if exists
        if market_pos.down_position:
            logger.info(f"Closing DOWN position for {asset} ({reason})")
            pnl = await self._execute_trade(asset, Action.SELL_DOWN, market_data)
            if pnl is not None:
                total_pnl += pnl
        
        return total_pnl
    
    async def _execute_trade(
        self,
        asset: str,
        action: Action,
        market_data: dict,
        confidence: float = 0.0,
        exit_reason: str = None,
    ) -> Optional[float]:
        """
        Execute a trade.

        Returns P&L if closing a position, None otherwise.
        """
        if action == Action.HOLD:
            return None

        # Determine price and side
        if action in (Action.BUY_DOWN, Action.SELL_DOWN):
            side = "DOWN"
            if action == Action.BUY_DOWN:
                price = 1 - market_data["best_bid"]
            else:
                price = 1 - market_data["best_ask"]
        else:
            side = "UP"
            if action == Action.BUY_UP:
                price = market_data["best_ask"]
            else:
                price = market_data["best_bid"]

        pnl = None
        token_id = market_data["down_token_id"] if side == "DOWN" else market_data["up_token_id"]

        # === BUY ===
        if action in (Action.BUY_DOWN, Action.BUY_UP):
            # Confidence-based position sizing
            position_size = self._get_confidence_adjusted_size(confidence, POSITION_SIZE_USD)
            size_multiplier = position_size / POSITION_SIZE_USD
            logger.info(
                f"Confidence={confidence:.2f}, sizing={size_multiplier:.1f}x (${position_size:.2f})"
            )

            # Check exposure limits
            current_exposure = self.position_manager.get_total_exposure()
            if current_exposure + position_size > MAX_TOTAL_EXPOSURE:
                logger.warning(f"Would exceed max exposure (${current_exposure:.2f} + ${position_size:.2f} > ${MAX_TOTAL_EXPOSURE:.2f}), skipping")
                return None

            shares = position_size / price

            if self.dry_run:
                logger.info(f"[DRY RUN] BUY {side} for {asset} @ ${price:.4f} ({shares:.2f} shares, ${position_size:.2f})")
                record_dry_run_trade({
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "asset": asset,
                    "side": side,
                    "action": "BUY",
                    "price": price,
                    "size_usd": position_size,
                    "shares": shares,
                    "pnl": None,
                    "exit_reason": None,
                    "confidence": confidence,
                    "size_multiplier": size_multiplier,
                })
            else:
                result = await self.clob_client.place_order(
                    token_id=token_id,
                    side="BUY",
                    price=price,
                    size=position_size,
                )
                if not result.success:
                    logger.error(f"Failed to place BUY order for {asset} {side}: {result.error}")
                    return None
                # Slippage validation
                if result.filled_price > 0:
                    slippage = abs(result.filled_price - price) / price * 100
                    if slippage > MAX_SLIPPAGE_PCT:
                        logger.warning(
                            f"SLIPPAGE ALERT {asset} {side}: expected ${price:.4f}, "
                            f"got ${result.filled_price:.4f} ({slippage:.1f}%)"
                        )
                position_size = result.filled_size * result.filled_price
                shares = result.filled_size
                price = result.filled_price
                if position_size <= 0:
                    return None
                logger.info(f"BUY filled for {asset} {side}: {result.filled_size:.2f} shares @ ${result.filled_price:.2f} (${position_size:.2f})")

            self.position_manager.open_position(asset, side, price, position_size)
            self.trade_logger.log_trade(asset, action, price, position_size)

        # === SELL ===
        else:
            market_pos = self.position_manager.get_position(asset)
            position = market_pos.up_position if side == "UP" else market_pos.down_position

            if position is None:
                logger.warning(f"No {side} position to sell for {asset}")
                return None

            shares = position.size

            if self.dry_run:
                logger.info(f"[DRY RUN] SELL {side} for {asset} @ ${price:.4f} ({shares:.2f} shares)")
                pnl = self.position_manager.close_position(asset, side, price)
                self.trade_logger.log_trade(asset, action, price, shares, pnl)
                record_dry_run_trade({
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "asset": asset,
                    "side": side,
                    "action": "SELL",
                    "price": price,
                    "size_usd": shares * price,
                    "shares": shares,
                    "pnl": pnl,
                    "exit_reason": exit_reason,
                    "confidence": None,
                    "size_multiplier": None,
                })
            else:
                result = await self.clob_client.place_order(
                    token_id=token_id,
                    side="SELL",
                    price=price,
                    size=shares,
                )
                if not result.success:
                    logger.error(f"Failed to place SELL order for {asset} {side}: {result.error}")
                    return None
                # Slippage validation
                if result.filled_price > 0:
                    slippage = abs(result.filled_price - price) / price * 100
                    if slippage > MAX_SLIPPAGE_PCT:
                        logger.warning(
                            f"SLIPPAGE ALERT {asset} {side}: expected ${price:.4f}, "
                            f"got ${result.filled_price:.4f} ({slippage:.1f}%)"
                        )
                shares_filled = result.filled_size
                price = result.filled_price
                if shares_filled <= 0:
                    return None
                logger.info(f"SELL filled for {asset} {side}: {shares_filled:.2f} shares @ ${price:.2f}")
                pnl = self.position_manager.reduce_position(asset, side, shares_filled, price)
                self.trade_logger.log_trade(asset, action, price, shares_filled, pnl)

        return pnl
    
    async def _process_asset(self, asset: str) -> Optional[float]:
        """
        Process one asset - fetch data, decide, and execute.

        Returns P&L if a trade with P&L was executed.
        """
        binance_symbol = ASSETS[asset]["binance"]
        polymarket_name = ASSETS[asset]["polymarket"]
        coingecko_id = ASSETS[asset]["coingecko"]

        # Fetch price data (with fallback sources)
        try:
            price_data = await self.price_fetcher.get_price_data(binance_symbol, coingecko_id)
            binance_pct = price_data["pct_change"]
            if price_data.get("source") and price_data["source"] != "binance.com":
                logger.info(f"Using {price_data['source']} for {asset} price data")
        except Exception as e:
            logger.error(f"Error fetching price data for {asset}: {e}")
            return None

        # Update trend history and get trend confidence (moving average)
        trend_confidence = self._update_trend_history(asset, binance_pct)

        # Fetch Polymarket market data
        market_data = await self.polymarket.get_market_data(polymarket_name)
        if market_data is None:
            logger.warning(f"No market data for {asset}")
            return None

        # Get current state
        state = self.position_manager.get_state(asset)
        minutes_remaining = self._get_minutes_remaining()

        # Calculate position P&L and get entry price
        position_pnl_pct = 0.0
        entry_price = 0.5
        market_pos = self.position_manager.get_position(asset)
        if state == PositionState.LONG_DOWN:
            down_price = market_data["down_price"]
            position_pnl_pct = self.position_manager.get_position_pnl_pct(asset, "DOWN", down_price)
            if market_pos.down_position:
                entry_price = market_pos.down_position.entry_price
            # Update trailing stop tracker
            self.position_manager.update_peak_pnl(asset, position_pnl_pct)
        elif state == PositionState.LONG_UP:
            up_price = market_data["up_price"]
            position_pnl_pct = self.position_manager.get_position_pnl_pct(asset, "UP", up_price)
            if market_pos.up_position:
                entry_price = market_pos.up_position.entry_price
            # Update trailing stop tracker
            self.position_manager.update_peak_pnl(asset, position_pnl_pct)

        # Check cooldowns
        can_trade_cooldown = self.position_manager.can_cooldown_trade(asset)
        can_enter_up = self.position_manager.can_enter_side(asset, "UP")
        can_enter_down = self.position_manager.can_enter_side(asset, "DOWN")

        # Check if initial entry
        is_initial = self.is_initial_entry.get(asset, True)

        # Calculate confidence for potential entry
        target_side_price = market_data["down_price"] if binance_pct < 0 else (1.0 - market_data["down_price"])
        confidence = self.state_machine.calculate_confidence(
            binance_pct=binance_pct,
            polymarket_price=target_side_price,
            liquidity=market_data.get("liquidity", 0),
            minutes_remaining=minutes_remaining,
            trend_confidence=trend_confidence,
        )

        # Decide action
        action = self.state_machine.decide(
            state=state,
            binance_pct=binance_pct,
            position_pnl_pct=position_pnl_pct,
            minutes_remaining=minutes_remaining,
            spread=market_data["spread"],
            can_trade_cooldown=can_trade_cooldown,
            is_initial_entry=is_initial,
            down_price=market_data["down_price"],
            entry_price=entry_price,
            asset=asset,
            trend_confidence=trend_confidence,
            peak_pnl_pct=market_pos.peak_pnl_pct,
            can_enter_side_up=can_enter_up,
            can_enter_side_down=can_enter_down,
        )

        # Log state
        logger.debug(
            f"{asset}: state={state.value}, binance={binance_pct:+.2f}%, "
            f"pnl={position_pnl_pct:+.1f}%, confidence={confidence:.2f}, action={action.value}"
        )

        # Determine exit reason for SELL actions
        exit_reason = None
        if action in (Action.SELL_DOWN, Action.SELL_UP):
            if position_pnl_pct <= STOP_LOSS_PCT:
                exit_reason = "stop_loss"
            elif minutes_remaining <= CLOSE_POSITIONS_MINUTES:
                exit_reason = "hour_end"
            elif market_pos.peak_pnl_pct >= 10.0 and position_pnl_pct < market_pos.peak_pnl_pct - 5.0:
                exit_reason = "trailing_stop"
            elif position_pnl_pct >= self.state_machine.get_dynamic_profit_target(entry_price, minutes_remaining):
                exit_reason = "profit_target"
            else:
                exit_reason = "trend_reversal"

        # Execute if not HOLD
        pnl = None
        if action != Action.HOLD:
            pnl = await self._execute_trade(
                asset, action, market_data,
                confidence=confidence,
                exit_reason=exit_reason,
            )

            # Mark initial entry done
            if is_initial and action in (Action.BUY_DOWN, Action.BUY_UP):
                self.is_initial_entry[asset] = False

        return pnl
    
    async def _redeem_resolved_positions(self):
        """
        Redeem any resolved positions to recover USDC.e.
        
        Called at the end of each hour to automatically claim winnings
        from markets that have resolved.
        """
        if self.dry_run:
            logger.debug("Skipping redemption in dry run mode")
            return
        
        try:
            # Initialize Web3
            w3 = Web3(Web3.HTTPProvider(POLYGON_RPC_URL, request_kwargs={'timeout': 30}))
            if not w3.is_connected():
                logger.warning("Could not connect to Polygon RPC for redemption")
                return
            
            account = Account.from_key(config.PRIVATE_KEY)
            address = account.address
            
            # Fetch redeemable positions from Polymarket API
            async with httpx.AsyncClient(timeout=30.0) as client:
                response = await client.get(
                    f"{DATA_API_URL}/positions",
                    params={"user": address, "limit": 500, "sizeThreshold": 0.01}
                )
                if response.status_code != 200:
                    logger.warning(f"Failed to fetch positions for redemption: {response.status_code}")
                    return
                
                positions_data = response.json()
            
            # Filter to redeemable positions with value
            redeemable = [
                p for p in positions_data 
                if p.get("redeemable", False) and float(p.get("currentValue", 0)) >= MIN_REDEMPTION_VALUE
            ]
            
            if not redeemable:
                logger.debug("No redeemable positions with value found")
                return
            
            # Group by condition_id
            condition_ids = {}
            for p in redeemable:
                cid = p.get("conditionId", "")
                if not cid:
                    continue
                if cid not in condition_ids:
                    condition_ids[cid] = {
                        'title': p.get("title", "Unknown"),
                        'total_value': 0
                    }
                condition_ids[cid]['total_value'] += float(p.get("currentValue", 0))
            
            # Sort by value (highest first)
            sorted_conditions = sorted(
                condition_ids.items(),
                key=lambda x: x[1]['total_value'],
                reverse=True
            )
            
            total_to_redeem = sum(data['total_value'] for _, data in sorted_conditions)
            logger.info(f"[REDEMPTION] Found {len(sorted_conditions)} positions to redeem (${total_to_redeem:.2f})")
            
            # Get initial balance
            usdc = w3.eth.contract(address=USDC_E_ADDRESS, abi=USDC_BALANCE_ABI)
            balance_before = usdc.functions.balanceOf(address).call() / 1e6
            
            # Setup CTF contract
            ctf = w3.eth.contract(address=CONDITIONAL_TOKENS_ADDRESS, abi=CTF_REDEEM_ABI)
            
            success_count = 0
            total_recovered = 0.0
            
            for idx, (cid, data) in enumerate(sorted_conditions[:10]):  # Limit to 10 per hour
                logger.info(f"[REDEMPTION] Redeeming: {data['title'][:40]}... (${data['total_value']:.2f})")
                
                # Add delay between transactions
                if idx > 0:
                    await asyncio.sleep(3)
                
                try:
                    # Get fresh nonce
                    nonce = w3.eth.get_transaction_count(address)
                    gas_price = int(w3.eth.gas_price * 1.3)
                    
                    # Build redemption transaction
                    tx = ctf.functions.redeemPositions(
                        USDC_E_ADDRESS,
                        bytes(32),  # parentCollectionId = 0
                        bytes.fromhex(cid[2:]),  # conditionId without 0x prefix
                        [1, 2]  # index sets for binary outcomes
                    ).build_transaction({
                        'from': address,
                        'nonce': nonce,
                        'gas': 200000,
                        'gasPrice': gas_price,
                        'chainId': 137,
                    })
                    
                    signed = account.sign_transaction(tx)
                    tx_hash = w3.eth.send_raw_transaction(signed.raw_transaction)
                    logger.info(f"[REDEMPTION] Tx sent: {tx_hash.hex()}")
                    
                    # Wait for confirmation
                    receipt = w3.eth.wait_for_transaction_receipt(tx_hash, timeout=60)
                    if receipt['status'] == 1:
                        logger.info(f"[REDEMPTION] ✓ Redeemed successfully!")
                        success_count += 1
                        total_recovered += data['total_value']
                    else:
                        logger.warning(f"[REDEMPTION] Transaction failed (status=0)")
                        
                except Exception as e:
                    error_msg = str(e)
                    if "execution reverted" in error_msg.lower():
                        logger.debug(f"[REDEMPTION] Skipped (may be neg risk market)")
                    elif "rate limit" in error_msg.lower():
                        logger.warning(f"[REDEMPTION] Rate limited, will retry next hour")
                        break
                    else:
                        logger.warning(f"[REDEMPTION] Error: {error_msg[:60]}")
            
            # Log summary
            if success_count > 0:
                # Get final balance
                await asyncio.sleep(2)
                balance_after = usdc.functions.balanceOf(address).call() / 1e6
                actual_recovered = balance_after - balance_before
                logger.info(
                    f"[REDEMPTION] Complete: {success_count}/{len(sorted_conditions)} redeemed, "
                    f"${actual_recovered:.2f} recovered"
                )
            
        except Exception as e:
            logger.error(f"[REDEMPTION] Error during redemption: {e}")
    
    async def _check_new_hour(self):
        """Check if we've entered a new hour and reset state."""
        current_hour = self._get_current_et_hour()
        
        if self.current_hour is None:
            self.current_hour = current_hour
            for asset in ASSETS:
                self.trade_logger.start_session(asset, current_hour)
                self.is_initial_entry[asset] = True
            return
        
        if current_hour > self.current_hour:
            logger.info(f"New hour detected: {current_hour}")
            
            # Check for any unclosed positions - attempt to determine resolution outcome
            for asset in ASSETS:
                market_pos = self.position_manager.get_position(asset)
                if market_pos.up_position or market_pos.down_position:
                    await self._check_resolution_outcome(asset, market_pos)
            
            # Save logs for previous hour
            for asset in ASSETS:
                self.trade_logger.save_session(asset)
            
            # Cancel all stale open orders to free locked USDC
            await self._cancel_all_open_orders()
            
            # Redeem any resolved positions from previous hours
            # This runs asynchronously and won't block the main loop
            try:
                await self._redeem_resolved_positions()
            except Exception as e:
                logger.error(f"Error in redemption: {e}")
            
            # Reset for new hour - carry forward last 2 trend readings for continuity
            for asset_key in list(self._trend_history.keys()):
                if len(self._trend_history[asset_key]) > 2:
                    self._trend_history[asset_key] = self._trend_history[asset_key][-2:]
            self.position_manager.clear_all()
            self.current_hour = current_hour
            
            for asset in ASSETS:
                self.trade_logger.start_session(asset, current_hour)
                self.is_initial_entry[asset] = True
                # Pre-seed trend history with current Binance data
                await self._seed_trend_from_kline(asset)

    async def _check_resolution_outcome(self, asset: str, market_pos: MarketPosition):
        """
        Check the resolution outcome for positions that went to hour end.
        Queries the previous hour's market to determine which side won.
        """
        polymarket_name = ASSETS[asset]["polymarket"]
        try:
            # The previous hour's market should now be resolved or resolving
            # Fetch the market data - if prices are near 0/1, we can determine outcome
            market_data = await self.polymarket.get_market_data(polymarket_name)
            if market_data is None:
                logger.warning(f"[RESOLUTION] Could not fetch market data for {asset} resolution check")
                return

            up_price = market_data.get("up_price", 0.5)
            down_price = market_data.get("down_price", 0.5)

            # Determine winner based on final prices
            up_won = up_price > 0.8
            down_won = down_price > 0.8
            outcome = "UP" if up_won else ("DOWN" if down_won else "UNKNOWN")

            total_pnl = 0.0
            if market_pos.up_position:
                final_price = 1.0 if up_won else (0.0 if down_won else up_price)
                pnl = (market_pos.up_position.size * final_price) - market_pos.up_position.cost
                total_pnl += pnl
                logger.info(f"[RESOLUTION] {asset} UP: {'WON' if up_won else 'LOST'}, P&L: ${pnl:.2f}")

            if market_pos.down_position:
                final_price = 1.0 if down_won else (0.0 if up_won else down_price)
                pnl = (market_pos.down_position.size * final_price) - market_pos.down_position.cost
                total_pnl += pnl
                logger.info(f"[RESOLUTION] {asset} DOWN: {'WON' if down_won else 'LOST'}, P&L: ${pnl:.2f}")

            logger.info(f"[RESOLUTION] {asset} outcome={outcome}, total P&L: ${total_pnl:.2f}")

            # Record in trade logger
            if market_pos.up_position:
                self.trade_logger.log_trade(asset, Action.SELL_UP, up_price, market_pos.up_position.size, total_pnl if not market_pos.down_position else None)
            if market_pos.down_position:
                self.trade_logger.log_trade(asset, Action.SELL_DOWN, down_price, market_pos.down_position.size, total_pnl if not market_pos.up_position else None)

        except Exception as e:
            logger.error(f"[RESOLUTION] Error checking outcome for {asset}: {e}")
            positions = []
            if market_pos.up_position:
                positions.append(f"UP (cost=${market_pos.up_position.cost:.2f})")
            if market_pos.down_position:
                positions.append(f"DOWN (cost=${market_pos.down_position.cost:.2f})")
            logger.warning(f"[RESOLUTION] {asset} had unclosed positions: {', '.join(positions)}")

    async def _seed_trend_from_kline(self, asset: str):
        """Pre-seed trend history with Binance candle data for current hour."""
        binance_symbol = ASSETS[asset]["binance"]
        coingecko_id = ASSETS[asset]["coingecko"]
        try:
            price_data = await self.price_fetcher.get_price_data(binance_symbol, coingecko_id)
            initial_pct = price_data["pct_change"]
            if asset not in self._trend_history:
                self._trend_history[asset] = []
            self._trend_history[asset].append(initial_pct)
            logger.debug(f"Seeded trend for {asset}: {initial_pct:+.2f}%")
        except Exception as e:
            logger.warning(f"Could not seed trend for {asset}: {e}")

    async def run_once(self):
        """Run one iteration of the main loop."""
        await self._check_new_hour()
        
        for asset in ASSETS:
            try:
                await self._process_asset(asset)
            except Exception as e:
                logger.error(f"Error processing {asset}: {e}")
    
    async def run(self):
        """Main loop - runs continuously."""
        logger.info("Starting hourly trading bot...")
        
        await self.initialize()
        
        try:
            while True:
                await self.run_once()
                await asyncio.sleep(POLL_INTERVAL_SEC)
                
        except KeyboardInterrupt:
            logger.info("Shutting down...")
        finally:
            # Save final logs
            for asset in ASSETS:
                self.trade_logger.save_session(asset)
            await self.close()


# =============================================================================
# Entry Point
# =============================================================================

async def main():
    """Main entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Hourly Crypto Trading Bot")
    parser.add_argument("--live", action="store_true", help="Run in live mode (execute real trades)")
    parser.add_argument("--once", action="store_true", help="Run once and exit")
    args = parser.parse_args()
    
    dry_run = not args.live
    
    if not dry_run:
        logger.warning("=" * 60)
        logger.warning("RUNNING IN LIVE MODE - REAL TRADES WILL BE EXECUTED")
        logger.warning("=" * 60)
        await asyncio.sleep(3)  # Give time to cancel
    
    bot = HourlyTradingBot(dry_run=dry_run)
    
    if args.once:
        await bot.initialize()
        await bot.run_once()
        await bot.close()
    else:
        await bot.run()


if __name__ == "__main__":
    asyncio.run(main())
