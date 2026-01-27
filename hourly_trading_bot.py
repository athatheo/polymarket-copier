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
from datetime import datetime, timezone, timedelta
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional
from pathlib import Path

import config
from api.clob_client import ClobClient

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
HEDGE_TRIGGER_PCT = float(getattr(config, 'HOURLY_HEDGE_TRIGGER_PCT', -10))
RE_ENTRY_COOLDOWN_SEC = int(getattr(config, 'HOURLY_RE_ENTRY_COOLDOWN_SEC', 30))
POLL_INTERVAL_SEC = int(getattr(config, 'HOURLY_POLL_INTERVAL_SEC', 15))
MAX_SPREAD_TO_ENTER = float(getattr(config, 'HOURLY_MAX_SPREAD_TO_ENTER', 0.03))
MIN_MINUTES_TO_TRADE = int(getattr(config, 'HOURLY_MIN_MINUTES_TO_TRADE', 5))

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
# Enums and Data Classes
# =============================================================================

class PositionState(Enum):
    NONE = "NONE"
    LONG_DOWN = "LONG_DOWN"
    LONG_UP = "LONG_UP"
    HEDGED = "HEDGED"


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
    """Tracks positions for a single market (can have both UP and DOWN)."""
    up_position: Optional[Position] = None
    down_position: Optional[Position] = None
    last_exit_time: Optional[datetime] = None
    
    def get_state(self) -> PositionState:
        has_up = self.up_position is not None
        has_down = self.down_position is not None
        
        if has_up and has_down:
            return PositionState.HEDGED
        elif has_up:
            return PositionState.LONG_UP
        elif has_down:
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
        # Calculate current ET hour
        utc_now = datetime.now(timezone.utc)
        et_offset = timedelta(hours=-5)  # ET is UTC-5
        et_now = utc_now + et_offset
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
        
        market_pos.last_exit_time = datetime.now(timezone.utc)
        
        logger.info(f"Closed {side} position for {asset}: {position.size:.2f} shares @ ${exit_price:.4f}, P&L: ${pnl:.2f}")
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
    
    def clear_all(self):
        """Clear all positions (for new hour)."""
        self.positions.clear()


# =============================================================================
# TradingStateMachine - Core decision logic
# =============================================================================

class TradingStateMachine:
    """Makes trading decisions based on current state and market conditions."""
    
    def decide(
        self,
        state: PositionState,
        binance_pct: float,
        position_pnl_pct: float,
        minutes_remaining: int,
        spread: float,
        can_trade_cooldown: bool,
        is_initial_entry: bool = False,
    ) -> Action:
        """
        Decide what action to take.
        
        Args:
            state: Current position state
            binance_pct: % change of underlying from hourly open
            position_pnl_pct: Current P&L % of position (if any)
            minutes_remaining: Minutes until hour ends
            spread: Current bid-ask spread
            can_trade_cooldown: Whether cooldown period has passed
            is_initial_entry: Whether this is the initial entry at hour start
        
        Returns:
            Action to take
        """
        # Don't trade in last few minutes
        if minutes_remaining < MIN_MINUTES_TO_TRADE:
            # Only allow closing positions, not opening new ones
            if state == PositionState.NONE:
                return Action.HOLD
        
        # Check spread - don't enter if too wide
        if spread > MAX_SPREAD_TO_ENTER and state == PositionState.NONE:
            logger.debug(f"Spread {spread:.4f} too wide, holding")
            return Action.HOLD
        
        # === NO POSITION - Look for entry ===
        if state == PositionState.NONE:
            if not can_trade_cooldown:
                return Action.HOLD
            
            # Initial entry: bias towards Down
            if is_initial_entry:
                return Action.BUY_DOWN
            
            # Re-entry: follow the trend
            if binance_pct < -0.1:  # Price trending down
                return Action.BUY_DOWN
            elif binance_pct > 0.1:  # Price trending up
                return Action.BUY_UP
            else:
                # Flat - default to Down (original strategy bias)
                return Action.BUY_DOWN
        
        # === HOLDING DOWN - Check for profit-take or hedge ===
        elif state == PositionState.LONG_DOWN:
            # Profit take
            if position_pnl_pct >= PROFIT_TAKE_PCT:
                logger.info(f"Taking profit on DOWN: {position_pnl_pct:.1f}%")
                return Action.SELL_DOWN
            
            # Hedge if losing significantly
            if position_pnl_pct <= HEDGE_TRIGGER_PCT:
                logger.info(f"Hedging DOWN position: {position_pnl_pct:.1f}%")
                return Action.BUY_UP
            
            return Action.HOLD
        
        # === HOLDING UP - Check for profit-take or hedge ===
        elif state == PositionState.LONG_UP:
            # Profit take
            if position_pnl_pct >= PROFIT_TAKE_PCT:
                logger.info(f"Taking profit on UP: {position_pnl_pct:.1f}%")
                return Action.SELL_UP
            
            # Hedge if losing significantly
            if position_pnl_pct <= HEDGE_TRIGGER_PCT:
                logger.info(f"Hedging UP position: {position_pnl_pct:.1f}%")
                return Action.BUY_DOWN
            
            return Action.HOLD
        
        # === HEDGED - Check if we can exit both profitably or cut losses ===
        elif state == PositionState.HEDGED:
            # In hedged state, we need to make a decision near hour end
            # or if one side is clearly winning
            
            if minutes_remaining < 10:
                # Near hour end - pick the winning side
                if binance_pct < -0.2:
                    # Down likely to win - sell Up, hold Down
                    return Action.SELL_UP
                elif binance_pct > 0.2:
                    # Up likely to win - sell Down, hold Up
                    return Action.SELL_DOWN
            
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
    
    async def initialize(self):
        """Initialize all clients."""
        self.http = httpx.AsyncClient(timeout=30.0)
        self.price_fetcher = PriceFetcher(self.http)
        self.polymarket = PolymarketFetcher(self.http)
        
        if not self.dry_run:
            self.clob_client = ClobClient()
            await self.clob_client.initialize()
        
        logger.info(f"Bot initialized (dry_run={self.dry_run})")
    
    async def close(self):
        """Clean up resources."""
        if self.http:
            await self.http.aclose()
        if self.clob_client:
            await self.clob_client.close()
    
    def _get_current_et_hour(self) -> datetime:
        """Get the current hour in ET."""
        utc_now = datetime.now(timezone.utc)
        et_offset = timedelta(hours=-5)
        et_now = utc_now + et_offset
        return et_now.replace(minute=0, second=0, microsecond=0)
    
    def _get_minutes_remaining(self) -> int:
        """Get minutes remaining in current hour."""
        utc_now = datetime.now(timezone.utc)
        et_offset = timedelta(hours=-5)
        et_now = utc_now + et_offset
        return 60 - et_now.minute
    
    async def _execute_trade(
        self,
        asset: str,
        action: Action,
        market_data: dict,
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
            # For DOWN: buy at down_ask (1 - up_bid), sell at down_bid (1 - up_ask)
            if action == Action.BUY_DOWN:
                price = 1 - market_data["best_bid"]  # Cost to buy Down
            else:
                price = 1 - market_data["best_ask"]  # Receive when selling Down
        else:
            side = "UP"
            if action == Action.BUY_UP:
                price = market_data["best_ask"]  # Cost to buy Up
            else:
                price = market_data["best_bid"]  # Receive when selling Up
        
        pnl = None
        
        # Get the token ID for the side we're trading
        token_id = market_data["down_token_id"] if side == "DOWN" else market_data["up_token_id"]
        
        # Execute the trade
        if action in (Action.BUY_DOWN, Action.BUY_UP):
            # Check exposure limits
            current_exposure = self.position_manager.get_total_exposure()
            if current_exposure + POSITION_SIZE_USD > MAX_TOTAL_EXPOSURE:
                logger.warning(f"Would exceed max exposure, skipping {action.value}")
                return None
            
            # Calculate shares to buy
            shares = POSITION_SIZE_USD / price
            
            if self.dry_run:
                logger.info(f"[DRY RUN] Would BUY {side} for {asset} @ ${price:.4f} ({shares:.2f} shares)")
            else:
                # Execute actual order via CLOB
                result = await self.clob_client.place_order(
                    token_id=token_id,
                    side="BUY",
                    price=price,
                    size=shares,
                )
                if not result.success:
                    logger.error(f"Failed to place BUY order for {asset} {side}: {result.error}")
                    return None
                logger.info(f"BUY order placed for {asset} {side}: {result.order_id}")
            
            self.position_manager.open_position(asset, side, price, POSITION_SIZE_USD)
            self.trade_logger.log_trade(asset, action, price, POSITION_SIZE_USD)
            
        else:  # SELL
            # Get the position to know how many shares to sell
            market_pos = self.position_manager.get_position(asset)
            position = market_pos.up_position if side == "UP" else market_pos.down_position
            
            if position is None:
                logger.warning(f"No {side} position to sell for {asset}")
                return None
            
            shares = position.size
            
            if self.dry_run:
                logger.info(f"[DRY RUN] Would SELL {side} for {asset} @ ${price:.4f} ({shares:.2f} shares)")
            else:
                # Execute actual order via CLOB
                result = await self.clob_client.place_order(
                    token_id=token_id,
                    side="SELL",
                    price=price,
                    size=shares,
                )
                if not result.success:
                    logger.error(f"Failed to place SELL order for {asset} {side}: {result.error}")
                    return None
                logger.info(f"SELL order placed for {asset} {side}: {result.order_id}")
            
            pnl = self.position_manager.close_position(asset, side, price)
            self.trade_logger.log_trade(asset, action, price, shares, pnl)
        
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
        
        # Fetch Polymarket market data
        market_data = await self.polymarket.get_market_data(polymarket_name)
        if market_data is None:
            logger.warning(f"No market data for {asset}")
            return None
        
        # Get current state
        state = self.position_manager.get_state(asset)
        
        # Calculate position P&L if we have a position
        position_pnl_pct = 0.0
        if state == PositionState.LONG_DOWN:
            down_price = market_data["down_price"]
            position_pnl_pct = self.position_manager.get_position_pnl_pct(asset, "DOWN", down_price)
        elif state == PositionState.LONG_UP:
            up_price = market_data["up_price"]
            position_pnl_pct = self.position_manager.get_position_pnl_pct(asset, "UP", up_price)
        
        # Get timing info
        minutes_remaining = self._get_minutes_remaining()
        
        # Check cooldown
        can_trade_cooldown = self.position_manager.can_cooldown_trade(asset)
        
        # Check if initial entry
        is_initial = self.is_initial_entry.get(asset, True)
        
        # Decide action
        action = self.state_machine.decide(
            state=state,
            binance_pct=binance_pct,
            position_pnl_pct=position_pnl_pct,
            minutes_remaining=minutes_remaining,
            spread=market_data["spread"],
            can_trade_cooldown=can_trade_cooldown,
            is_initial_entry=is_initial,
        )
        
        # Log state
        logger.debug(
            f"{asset}: state={state.value}, binance={binance_pct:+.2f}%, "
            f"pnl={position_pnl_pct:+.1f}%, action={action.value}"
        )
        
        # Execute if not HOLD
        pnl = None
        if action != Action.HOLD:
            pnl = await self._execute_trade(asset, action, market_data)
            
            # Mark initial entry done
            if is_initial and action in (Action.BUY_DOWN, Action.BUY_UP):
                self.is_initial_entry[asset] = False
        
        return pnl
    
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
            
            # Save logs for previous hour
            for asset in ASSETS:
                self.trade_logger.save_session(asset)
            
            # Reset for new hour
            self.position_manager.clear_all()
            self.current_hour = current_hour
            
            for asset in ASSETS:
                self.trade_logger.start_session(asset, current_hour)
                self.is_initial_entry[asset] = True
    
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
