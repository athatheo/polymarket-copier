#!/usr/bin/env python3
"""
BTC Up/Down Hybrid Arbitrage Bot

Strategy:
1. Buy one side when cheap (e.g., UP at 23c)
2. If it increases in value → sell for profit
3. If it decreases → buy other side to complete arbitrage (guaranteed $1 payout)

Either way, you win.

Based on: https://polymarket.com/@nobuyoshi005
"""

import asyncio
import json
from dataclasses import dataclass, asdict, field
from datetime import datetime, timezone, timedelta
from typing import Optional
from pathlib import Path
from enum import Enum

import httpx
from dotenv import load_dotenv
load_dotenv()

from api.clob_client import ClobClient
import config

# =============================================================================
# CONFIGURATION
# =============================================================================

# Entry conditions - LIMIT ORDER STRATEGY
# Place limit orders at these prices and wait for fills
TARGET_ENTRY_PRICE = 0.35       # Price to place limit buy orders
MAX_ENTRY_ASK = 0.50            # Only enter if current ask is reasonable (shows some liquidity)
MIN_MINUTES_TO_EXPIRY = 8       # Don't enter too close to expiry

# Profit taking (exit via selling)
PROFIT_TARGET_PCT = 25          # Sell if up 25%
MIN_PROFIT_TO_SELL = 0.02       # Minimum $0.02 profit to consider selling

# Arbitrage completion (exit via buying other side)
MAX_COMBINED_FOR_ARB = 0.92     # Complete arb if combined cost < 92c (8% profit)

# Stop loss / time exit
STOP_LOSS_PCT = -30             # Cut loss if down 30%
FORCE_EXIT_MINUTES = 3          # Force exit before expiry (avoid binary)

# Liquidity thresholds
MIN_BID_FOR_LIQUIDITY = 0.05    # Consider market liquid if bid > 5c

# Position sizing
POSITION_SIZE_USD = 25          # USD per position

# Scan settings
SCAN_INTERVAL = 10              # Seconds between scans

GAMMA_API_URL = "https://gamma-api.polymarket.com"
CLOB_API_URL = "https://clob.polymarket.com"
POSITIONS_FILE = "hybrid_positions.json"

# Crypto Price tracking
TREND_WINDOW_MINUTES = 5        # Look at price change over last 5 minutes
MIN_TREND_PCT = 0.05            # Minimum 0.05% move to consider a trend
TREND_BOOST_THRESHOLD = 0.15    # If trend > 0.15%, strongly prefer that side

# Supported cryptocurrencies
SUPPORTED_CRYPTOS = ["BTC", "ETH", "SOL"]


# =============================================================================
# DATA CLASSES
# =============================================================================

class PositionStatus(str, Enum):
    OPEN = "open"           # Holding one side
    ARB_COMPLETE = "arb"    # Both sides bought, waiting for payout
    SOLD = "sold"           # Sold for profit
    STOPPED = "stopped"     # Stop loss triggered
    EXPIRED = "expired"     # Held to expiry


@dataclass
class CryptoMarket:
    """A Crypto Up/Down market with orderbook data."""
    event_id: str
    market_id: str
    title: str
    crypto: str  # "BTC", "ETH", or "SOL"
    end_time: datetime
    up_token_id: str
    down_token_id: str
    # Orderbook prices
    up_ask: Optional[float] = None  # Price to BUY up
    up_bid: Optional[float] = None  # Price to SELL up
    down_ask: Optional[float] = None
    down_bid: Optional[float] = None
    # Mid prices (estimates)
    up_mid: float = 0.5
    down_mid: float = 0.5


@dataclass 
class Position:
    """A position in one side of a market."""
    id: str
    market_id: str
    market_title: str
    crypto: str  # "BTC", "ETH", or "SOL"
    end_time: str  # ISO format
    
    # What we hold
    side: str  # "UP" or "DOWN"
    token_id: str
    entry_price: float
    size: float  # Number of shares
    entry_time: str
    
    # Other side info (for arb completion)
    other_side: str
    other_token_id: str
    
    # Current state
    status: PositionStatus = PositionStatus.OPEN
    
    # If arb completed
    other_entry_price: Optional[float] = None
    other_size: Optional[float] = None
    
    # Exit info
    exit_price: Optional[float] = None
    exit_time: Optional[str] = None
    exit_reason: Optional[str] = None
    
    # P&L
    realized_pnl: float = 0
    
    def to_dict(self):
        d = asdict(self)
        d['status'] = self.status.value
        return d
    
    @classmethod
    def from_dict(cls, d):
        d['status'] = PositionStatus(d['status'])
        return cls(**d)


# =============================================================================
# CRYPTO PRICE TRACKING
# =============================================================================

# Store recent prices for trend calculation (per crypto)
_crypto_price_history: dict[str, list[tuple[datetime, float]]] = {
    "BTC": [],
    "ETH": [],
    "SOL": [],
}


async def fetch_crypto_prices() -> dict[str, float]:
    """Fetch current prices for BTC, ETH, SOL from Kraken."""
    prices = {"BTC": 0, "ETH": 0, "SOL": 0}
    
    # Kraken pair mappings
    kraken_pairs = {
        "XXBTZUSD": "BTC",
        "XETHZUSD": "ETH",
        "SOLUSD": "SOL",
    }
    
    async with httpx.AsyncClient(timeout=10) as client:
        try:
            pair_list = ",".join(kraken_pairs.keys())
            response = await client.get(
                "https://api.kraken.com/0/public/Ticker",
                params={"pair": pair_list}
            )
            if response.status_code == 200:
                data = response.json()
                result = data.get("result", {})
                for kraken_pair, crypto in kraken_pairs.items():
                    if kraken_pair in result:
                        prices[crypto] = float(result[kraken_pair]["c"][0])
        except Exception as e:
            print(f"  Warning: Kraken price fetch failed: {e}")
        
        # Fallback to CryptoCompare for any missing prices
        missing = [c for c, p in prices.items() if p == 0]
        if missing:
            try:
                response = await client.get(
                    "https://min-api.cryptocompare.com/data/pricemulti",
                    params={"fsyms": ",".join(missing), "tsyms": "USD"}
                )
                if response.status_code == 200:
                    data = response.json()
                    for crypto in missing:
                        if crypto in data:
                            prices[crypto] = float(data[crypto].get("USD", 0))
            except:
                pass
    
    return prices


def update_crypto_price_history(prices: dict[str, float]):
    """Add prices to history and prune old entries."""
    global _crypto_price_history
    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(minutes=30)
    
    for crypto, price in prices.items():
        if price > 0:
            _crypto_price_history[crypto].append((now, price))
            # Prune old entries
            _crypto_price_history[crypto] = [
                (t, p) for t, p in _crypto_price_history[crypto] if t > cutoff
            ]


def get_crypto_trend(crypto: str) -> dict:
    """
    Calculate price trend for a specific crypto.
    
    Returns:
        {
            "current_price": float,
            "reference_price": float,
            "change_pct": float,
            "trend": str,              # "UP", "DOWN", or "FLAT"
            "strength": str,           # "STRONG", "MODERATE", "WEAK"
        }
    """
    history = _crypto_price_history.get(crypto, [])
    
    if len(history) < 2:
        return {
            "current_price": history[-1][1] if history else 0,
            "reference_price": 0,
            "change_pct": 0,
            "trend": "UNKNOWN",
            "strength": "NONE",
        }
    
    now = datetime.now(timezone.utc)
    current_price = history[-1][1]
    
    # Find price from X minutes ago
    target_time = now - timedelta(minutes=TREND_WINDOW_MINUTES)
    reference_price = current_price
    
    for timestamp, price in history:
        if timestamp <= target_time:
            reference_price = price
            break
    else:
        reference_price = history[0][1]
    
    # Calculate change
    if reference_price > 0:
        change_pct = ((current_price - reference_price) / reference_price) * 100
    else:
        change_pct = 0
    
    # Determine trend
    if abs(change_pct) < MIN_TREND_PCT:
        trend = "FLAT"
        strength = "NONE"
    elif change_pct > 0:
        trend = "UP"
        strength = "STRONG" if change_pct >= TREND_BOOST_THRESHOLD else "MODERATE" if change_pct >= MIN_TREND_PCT else "WEAK"
    else:
        trend = "DOWN"
        strength = "STRONG" if abs(change_pct) >= TREND_BOOST_THRESHOLD else "MODERATE" if abs(change_pct) >= MIN_TREND_PCT else "WEAK"
    
    return {
        "crypto": crypto,
        "current_price": current_price,
        "reference_price": reference_price,
        "change_pct": change_pct,
        "trend": trend,
        "strength": strength,
    }


def get_all_crypto_trends() -> dict[str, dict]:
    """Get trends for all supported cryptos."""
    return {crypto: get_crypto_trend(crypto) for crypto in SUPPORTED_CRYPTOS}


# =============================================================================
# MARKET SEARCH & PARSING
# =============================================================================

def get_search_terms():
    """Generate search terms for all cryptos for today and tomorrow."""
    now = datetime.now()
    terms = []
    
    crypto_names = {
        "BTC": ["Bitcoin", "BTC"],
        "ETH": ["Ethereum", "ETH"],
        "SOL": ["Solana", "SOL"],
    }
    
    for day_offset in range(0, 2):
        d = now + timedelta(days=day_offset)
        month = d.strftime("%B")
        day = d.day
        
        for crypto, names in crypto_names.items():
            for name in names:
                terms.append(f"{name} Up or Down {month} {day}")
    
    return terms


def identify_crypto(title: str) -> Optional[str]:
    """Identify which crypto a market is for based on title."""
    title_lower = title.lower()
    
    if "bitcoin" in title_lower or "btc" in title_lower:
        return "BTC"
    elif "ethereum" in title_lower or "eth" in title_lower:
        return "ETH"
    elif "solana" in title_lower or "sol" in title_lower:
        return "SOL"
    
    return None


async def search_crypto_markets() -> list[dict]:
    """Search for all crypto Up/Down markets (BTC, ETH, SOL)."""
    all_events = []
    
    async with httpx.AsyncClient(timeout=30) as client:
        for term in get_search_terms():
            try:
                response = await client.get(
                    f"{GAMMA_API_URL}/public-search",
                    params={
                        "q": term,
                        "limit_per_type": 50,
                        "search_tags": "false",
                        "search_profiles": "false",
                    }
                )
                response.raise_for_status()
                data = response.json()
                
                for event in data.get("events", []) or []:
                    title = event.get("title", "").lower()
                    if "up or down" in title:
                        crypto = identify_crypto(title)
                        if crypto:
                            event["_crypto"] = crypto  # Tag with crypto type
                            all_events.append(event)
            except Exception as e:
                print(f"  Search error for '{term}': {e}")
    
    # Deduplicate
    seen = set()
    unique = []
    for event in all_events:
        eid = event.get("id")
        if eid and eid not in seen:
            seen.add(eid)
            unique.append(event)
    
    return unique


def parse_market(event: dict, min_minutes: int = MIN_MINUTES_TO_EXPIRY) -> Optional[CryptoMarket]:
    """Parse event into CryptoMarket."""
    markets = event.get("markets", [])
    if not markets:
        return None
    
    # Get crypto type (tagged during search or identify from title)
    crypto = event.get("_crypto") or identify_crypto(event.get("title", ""))
    if not crypto:
        return None
    
    market = markets[0]
    end_str = market.get("endDate") or event.get("endDate")
    if not end_str:
        return None
    
    try:
        end_time = datetime.fromisoformat(end_str.replace("Z", "+00:00"))
    except:
        return None
    
    now = datetime.now(timezone.utc)
    mins = (end_time - now).total_seconds() / 60
    
    if mins < min_minutes or mins > 120:  # 2 hour max
        return None
    
    token_ids_str = market.get("clobTokenIds", "")
    try:
        token_ids = json.loads(token_ids_str) if token_ids_str else []
    except:
        return None
    
    if len(token_ids) < 2:
        return None
    
    prices_str = market.get("outcomePrices", "")
    try:
        prices = [float(p) for p in json.loads(prices_str)] if prices_str else [0.5, 0.5]
    except:
        prices = [0.5, 0.5]
    
    return CryptoMarket(
        event_id=event.get("id", ""),
        market_id=market.get("id", ""),
        title=event.get("title", ""),
        crypto=crypto,
        end_time=end_time,
        up_token_id=token_ids[0],
        down_token_id=token_ids[1],
        up_mid=prices[0] if prices else 0.5,
        down_mid=prices[1] if len(prices) > 1 else 0.5,
    )


async def fetch_orderbook(token_id: str) -> tuple[Optional[float], Optional[float]]:
    """Fetch best ask (buy price) and bid (sell price) for a token."""
    async with httpx.AsyncClient(timeout=10) as client:
        try:
            response = await client.get(
                f"{CLOB_API_URL}/book",
                params={"token_id": token_id}
            )
            if response.status_code == 200:
                book = response.json()
                asks = book.get("asks", [])
                bids = book.get("bids", [])
                
                best_ask = float(asks[0]["price"]) if asks else None
                best_bid = float(bids[0]["price"]) if bids else None
                
                return best_ask, best_bid
        except:
            pass
    return None, None


async def enrich_market(market: CryptoMarket) -> CryptoMarket:
    """Fetch orderbook data for market."""
    market.up_ask, market.up_bid = await fetch_orderbook(market.up_token_id)
    market.down_ask, market.down_bid = await fetch_orderbook(market.down_token_id)
    return market


# =============================================================================
# POSITION MANAGEMENT
# =============================================================================

def load_positions() -> list[Position]:
    """Load positions from file."""
    if not Path(POSITIONS_FILE).exists():
        return []
    try:
        with open(POSITIONS_FILE, "r") as f:
            return [Position.from_dict(p) for p in json.load(f)]
    except:
        return []


def save_positions(positions: list[Position]):
    """Save positions to file."""
    with open(POSITIONS_FILE, "w") as f:
        json.dump([p.to_dict() for p in positions], f, indent=2)


# =============================================================================
# TRADING LOGIC
# =============================================================================

async def execute_buy(
    clob_client: Optional[ClobClient],
    token_id: str,
    price: float,
    amount_usd: float,
    dry_run: bool = True
) -> tuple[bool, float]:
    """
    Execute a market buy.
    Returns (success, actual_size).
    """
    size = amount_usd / price
    
    print(f"      {'[DRY RUN] ' if dry_run else ''}BUY {size:.2f} shares @ ${price:.4f} = ${amount_usd:.2f}")
    
    if dry_run:
        return True, size
    
    if not clob_client:
        return False, 0
    
    try:
        result = await clob_client.place_order(
            token_id=token_id,
            side="BUY",
            price=price,
            size=size,
        )
        if result.success:
            print(f"        ✓ Order: {result.order_id}")
            return True, size
        else:
            print(f"        ✗ Failed: {result.error}")
    except Exception as e:
        print(f"        ✗ Error: {e}")
    
    return False, 0


async def execute_sell(
    clob_client: Optional[ClobClient],
    token_id: str,
    price: float,
    size: float,
    dry_run: bool = True
) -> bool:
    """Execute a market sell."""
    print(f"      {'[DRY RUN] ' if dry_run else ''}SELL {size:.2f} shares @ ${price:.4f}")
    
    if dry_run:
        return True
    
    if not clob_client:
        return False
    
    try:
        result = await clob_client.place_order(
            token_id=token_id,
            side="SELL",
            price=price,
            size=size,
        )
        if result.success:
            print(f"        ✓ Order: {result.order_id}")
            return True
        else:
            print(f"        ✗ Failed: {result.error}")
    except Exception as e:
        print(f"        ✗ Error: {e}")
    
    return False


def check_market_liquidity(market: CryptoMarket) -> dict:
    """
    Check if market has reasonable liquidity.
    Returns liquidity info.
    """
    up_liquid = market.up_bid and market.up_bid >= MIN_BID_FOR_LIQUIDITY
    down_liquid = market.down_bid and market.down_bid >= MIN_BID_FOR_LIQUIDITY
    
    # Also check if asks are reasonable (not just $0.99)
    up_ask_reasonable = market.up_ask and market.up_ask <= MAX_ENTRY_ASK
    down_ask_reasonable = market.down_ask and market.down_ask <= MAX_ENTRY_ASK
    
    return {
        "up_liquid": up_liquid,
        "down_liquid": down_liquid,
        "up_ask_reasonable": up_ask_reasonable,
        "down_ask_reasonable": down_ask_reasonable,
        "has_any_liquidity": up_liquid or down_liquid or up_ask_reasonable or down_ask_reasonable,
    }


def check_entry_opportunity(market: CryptoMarket, existing_positions: list[Position], crypto_trends: dict[str, dict]) -> Optional[dict]:
    """
    Check if we should enter a new position.
    
    Strategy:
    1. Use crypto price trend to choose which side to buy
    2. If crypto is going UP → prefer buying UP (aligned with momentum)
    3. If crypto is going DOWN → prefer buying DOWN
    4. Always ensure we can afford to cover with arbitrage if wrong
    """
    # Get trend for this market's crypto
    crypto_trend = crypto_trends.get(market.crypto, {"trend": "UNKNOWN", "strength": "NONE"})
    # Skip if we already have position in this market
    for pos in existing_positions:
        if pos.market_id == market.market_id and pos.status == PositionStatus.OPEN:
            return None
    
    now = datetime.now(timezone.utc)
    mins_left = (market.end_time - now).total_seconds() / 60
    
    if mins_left < MIN_MINUTES_TO_EXPIRY:
        return None
    
    # Need orderbook data
    if not market.up_ask or not market.down_ask:
        return None
    
    liquidity = check_market_liquidity(market)
    trend = crypto_trend.get("trend", "UNKNOWN")
    trend_strength = crypto_trend.get("strength", "NONE")
    
    # Determine preferred side based on BTC trend
    if trend == "UP":
        preferred_side = "UP"
        preferred_token = market.up_token_id
        preferred_ask = market.up_ask
        other_side = "DOWN"
        other_token = market.down_token_id
        other_ask = market.down_ask
    elif trend == "DOWN":
        preferred_side = "DOWN"
        preferred_token = market.down_token_id
        preferred_ask = market.down_ask
        other_side = "UP"
        other_token = market.up_token_id
        other_ask = market.up_ask
    else:
        # No clear trend - pick the cheaper side
        if market.up_ask <= market.down_ask:
            preferred_side = "UP"
            preferred_token = market.up_token_id
            preferred_ask = market.up_ask
            other_side = "DOWN"
            other_token = market.down_token_id
            other_ask = market.down_ask
        else:
            preferred_side = "DOWN"
            preferred_token = market.down_token_id
            preferred_ask = market.down_ask
            other_side = "UP"
            other_token = market.up_token_id
            other_ask = market.up_ask
    
    # Check if we can enter at a reasonable price
    # Strong trend = we're willing to pay more (up to MAX_ENTRY_ASK)
    # Weak/no trend = we want cheaper prices (TARGET_ENTRY_PRICE)
    
    max_price_for_entry = MAX_ENTRY_ASK if trend_strength == "STRONG" else TARGET_ENTRY_PRICE
    
    # Option 1: Direct entry if preferred side is cheap enough
    if preferred_ask <= max_price_for_entry:
        # Verify we could complete arb if needed (worst case cover)
        potential_combined = preferred_ask + other_ask
        can_cover = potential_combined <= 1.10  # Allow up to 10% loss on arb cover
        
        return {
            "side": preferred_side,
            "token_id": preferred_token,
            "price": preferred_ask,
            "order_type": "market",
            "other_side": other_side,
            "other_token_id": other_token,
            "mins_left": mins_left,
            "trend": trend,
            "trend_strength": trend_strength,
            "can_cover": can_cover,
        }
    
    # Option 2: If preferred side too expensive but other side is cheap, buy other side
    # (Sometimes the "wrong" side is so cheap it's worth it)
    if other_ask <= TARGET_ENTRY_PRICE * 0.8:  # Other side is very cheap
        return {
            "side": other_side,
            "token_id": other_token,
            "price": other_ask,
            "order_type": "market",
            "other_side": preferred_side,
            "other_token_id": preferred_token,
            "mins_left": mins_left,
            "trend": trend,
            "trend_strength": trend_strength,
            "note": "Counter-trend entry (very cheap)",
        }
    
    # Option 3: Place limit order if there's some liquidity
    if liquidity["has_any_liquidity"]:
        # Place limit at target price for preferred side
        if liquidity.get("up_liquid" if preferred_side == "UP" else "down_liquid"):
            return {
                "side": preferred_side,
                "token_id": preferred_token,
                "price": TARGET_ENTRY_PRICE,
                "order_type": "limit",
                "other_side": other_side,
                "other_token_id": other_token,
                "mins_left": mins_left,
                "trend": trend,
                "trend_strength": trend_strength,
            }
    
    return None


def check_exit_conditions(position: Position, market: CryptoMarket, crypto_trends: dict[str, dict]) -> Optional[dict]:
    """
    Check if we should exit a position.
    
    Uses crypto trend to decide:
    - Trend in our favor → hold longer, higher profit target
    - Trend against us → quick exit or arb cover
    
    Returns exit action if should exit:
    - {"action": "sell", "price": X, "reason": "..."}
    - {"action": "arb", "other_price": X, "reason": "..."}
    """
    now = datetime.now(timezone.utc)
    end_time = datetime.fromisoformat(position.end_time.replace("Z", "+00:00"))
    mins_left = (end_time - now).total_seconds() / 60
    
    # Get trend for this position's crypto
    crypto_trend = crypto_trends.get(position.crypto, {"trend": "UNKNOWN", "strength": "NONE"})
    trend = crypto_trend.get("trend", "UNKNOWN")
    trend_strength = crypto_trend.get("strength", "NONE")
    
    # Is trend in our favor?
    trend_in_favor = (position.side == "UP" and trend == "UP") or \
                     (position.side == "DOWN" and trend == "DOWN")
    trend_against = (position.side == "UP" and trend == "DOWN") or \
                    (position.side == "DOWN" and trend == "UP")
    
    # Get current prices for our side
    if position.side == "UP":
        current_bid = market.up_bid  # Price we can sell at
        other_ask = market.down_ask  # Price to complete arb
    else:
        current_bid = market.down_bid
        other_ask = market.up_ask
    
    if not current_bid:
        current_bid = position.entry_price * 0.5  # Assume worst case
    
    # Calculate P&L
    current_value = current_bid * position.size
    cost = position.entry_price * position.size
    pnl = current_value - cost
    pnl_pct = (pnl / cost) * 100 if cost > 0 else 0
    
    # === EXIT CONDITIONS (priority order) ===
    
    # 1. FORCE EXIT near expiry - always exit to avoid binary outcome
    if mins_left <= FORCE_EXIT_MINUTES:
        if current_bid and current_bid > 0.01:
            return {
                "action": "sell",
                "price": current_bid,
                "reason": f"FORCE_EXIT: {mins_left:.0f}min left, P&L: {pnl_pct:+.1f}%",
                "pnl": pnl,
            }
        # If we can't sell, try to arb
        if other_ask and (position.entry_price + other_ask) <= 1.05:
            return {
                "action": "arb",
                "other_price": other_ask,
                "reason": f"FORCE_ARB: {mins_left:.0f}min left, covering position",
                "arb_profit": 1.0 - (position.entry_price + other_ask),
            }
    
    # 2. PROFIT TARGET - dynamic based on trend
    # If trend in our favor, aim for higher profit
    profit_target = PROFIT_TARGET_PCT
    if trend_in_favor and trend_strength == "STRONG":
        profit_target = PROFIT_TARGET_PCT * 1.5  # 37.5% if strong trend in favor
    elif trend_against:
        profit_target = PROFIT_TARGET_PCT * 0.6  # 15% if trend against - take quick profit
    
    if pnl_pct >= profit_target and pnl >= MIN_PROFIT_TO_SELL:
        trend_note = " (trend in favor, held longer)" if trend_in_favor else ""
        return {
            "action": "sell",
            "price": current_bid,
            "reason": f"PROFIT: {pnl_pct:+.1f}% (${pnl:.2f}){trend_note}",
            "pnl": pnl,
        }
    
    # 3. TREND REVERSAL - if trend strongly against us and we have profit, take it
    if trend_against and trend_strength in ["STRONG", "MODERATE"] and pnl_pct > 5:
        return {
            "action": "sell",
            "price": current_bid,
            "reason": f"TREND_REVERSAL: BTC going {trend}, locking {pnl_pct:+.1f}%",
            "pnl": pnl,
        }
    
    # 4. ARBITRAGE COVER - if other side cheap enough for guaranteed profit
    if other_ask:
        combined_cost = position.entry_price + other_ask
        if combined_cost <= MAX_COMBINED_FOR_ARB:
            arb_profit = 1.0 - combined_cost
            return {
                "action": "arb",
                "other_price": other_ask,
                "reason": f"ARB_LOCK: {position.entry_price:.2f}+{other_ask:.2f}=${combined_cost:.2f}, profit ${arb_profit:.2f}",
                "arb_profit": arb_profit,
            }
    
    # 5. DEFENSIVE ARB - if trend strongly against us, cover even at smaller profit
    if trend_against and trend_strength == "STRONG" and other_ask:
        combined_cost = position.entry_price + other_ask
        if combined_cost <= 0.98:  # Accept smaller 2% profit to avoid loss
            arb_profit = 1.0 - combined_cost
            return {
                "action": "arb",
                "other_price": other_ask,
                "reason": f"DEFENSIVE_ARB: Trend against, locking ${arb_profit:.2f} profit",
                "arb_profit": arb_profit,
            }
    
    # 6. STOP LOSS - dynamic based on trend
    stop_loss = STOP_LOSS_PCT
    if trend_against and trend_strength == "STRONG":
        stop_loss = STOP_LOSS_PCT * 0.7  # Tighter stop (-21%) if trend strongly against
    
    if pnl_pct <= stop_loss:
        # Try to arb cover first if possible
        if other_ask and (position.entry_price + other_ask) <= 1.0:
            return {
                "action": "arb",
                "other_price": other_ask,
                "reason": f"STOP_ARB: Covering at breakeven instead of {pnl_pct:+.1f}% loss",
                "arb_profit": 1.0 - (position.entry_price + other_ask),
            }
        # Otherwise sell at loss
        if current_bid and current_bid > 0.01:
            return {
                "action": "sell",
                "price": current_bid,
                "reason": f"STOP_LOSS: {pnl_pct:+.1f}% (${pnl:.2f})",
                "pnl": pnl,
            }
    
    return None


# =============================================================================
# DISPLAY
# =============================================================================

def display_status(markets: list[CryptoMarket], positions: list[Position], crypto_trends: dict[str, dict]):
    """Display current status."""
    now = datetime.now(timezone.utc)
    
    print("\n" + "=" * 95)
    print(f"  CRYPTO HYBRID ARBITRAGE BOT - {now.strftime('%Y-%m-%d %H:%M:%S')} UTC")
    print("=" * 95)
    
    # All Crypto Prices & Trends
    print("\n💰 LIVE PRICES & TRENDS:")
    for crypto in SUPPORTED_CRYPTOS:
        trend_data = crypto_trends.get(crypto, {})
        trend = trend_data.get("trend", "UNKNOWN")
        strength = trend_data.get("strength", "NONE")
        price = trend_data.get("current_price", 0)
        change = trend_data.get("change_pct", 0)
        
        trend_emoji = {"UP": "📈", "DOWN": "📉", "FLAT": "➡️"}.get(trend, "❓")
        sign = "+" if change > 0 else ""
        
        print(f"   {crypto}: ${price:,.2f} | {trend_emoji} {trend} ({sign}{change:.3f}%) | {strength}")
    
    # Markets
    liquid_markets = []
    illiquid_markets = []
    
    for m in markets:
        liq = check_market_liquidity(m)
        if liq["has_any_liquidity"]:
            liquid_markets.append(m)
        else:
            illiquid_markets.append(m)
    
    print(f"\n📈 MARKETS WITH LIQUIDITY ({len(liquid_markets)})")
    print("-" * 95)
    
    if liquid_markets:
        print(f"{'Crypto':<5} {'Title':<38} {'Mins':>5} {'UP Ask':>8} {'UP Bid':>8} {'DN Ask':>8} {'DN Bid':>8} {'Status':>8}")
        print("-" * 95)
        for m in liquid_markets[:12]:
            mins = (m.end_time - now).total_seconds() / 60
            title = m.title[:35] + "..." if len(m.title) > 38 else m.title
            
            up_ask = f"${m.up_ask:.2f}" if m.up_ask else "N/A"
            up_bid = f"${m.up_bid:.2f}" if m.up_bid else "N/A"
            dn_ask = f"${m.down_ask:.2f}" if m.down_ask else "N/A"
            dn_bid = f"${m.down_bid:.2f}" if m.down_bid else "N/A"
            
            # Status
            if m.up_ask and m.up_ask <= TARGET_ENTRY_PRICE:
                status = "◀ BUY UP"
            elif m.down_ask and m.down_ask <= TARGET_ENTRY_PRICE:
                status = "◀ BUY DN"
            elif (m.up_bid and m.up_bid >= MIN_BID_FOR_LIQUIDITY) or (m.down_bid and m.down_bid >= MIN_BID_FOR_LIQUIDITY):
                status = "LIQUID"
            else:
                status = "OK"
            
            print(f"{m.crypto:<5} {title:<38} {mins:>4.0f}m {up_ask:>8} {up_bid:>8} {dn_ask:>8} {dn_bid:>8} {status:>8}")
    else:
        print("  ⚠️  No liquid markets found!")
        print("  Markets currently have $0.01/$0.99 spreads (no intermediate liquidity)")
        print("  This is common during off-hours (current: ~4AM ET)")
        print("  Better liquidity typically during US market hours (9AM-4PM ET)")
    
    if illiquid_markets:
        # Group by crypto
        by_crypto = {}
        for m in illiquid_markets:
            by_crypto.setdefault(m.crypto, []).append(m)
        
        print(f"\n📉 ILLIQUID MARKETS ({len(illiquid_markets)}) - $0.01/$0.99 spreads, waiting for activity")
        for crypto, markets_list in by_crypto.items():
            print(f"   {crypto}: {len(markets_list)} markets")
    
    # Positions
    open_pos = [p for p in positions if p.status == PositionStatus.OPEN]
    closed_pos = [p for p in positions if p.status != PositionStatus.OPEN]
    
    print(f"\n💼 OPEN POSITIONS ({len(open_pos)})")
    print("-" * 95)
    if open_pos:
        for pos in open_pos:
            end = datetime.fromisoformat(pos.end_time.replace("Z", "+00:00"))
            mins = (end - now).total_seconds() / 60
            print(f"  [{pos.crypto}] {pos.market_title[:45]}")
            print(f"    {pos.side} @ ${pos.entry_price:.4f} x {pos.size:.1f} shares | {mins:.0f}min left")
    else:
        print("  None")
    
    print(f"\n📊 CLOSED POSITIONS ({len(closed_pos)})")
    print("-" * 95)
    if closed_pos:
        total_pnl = sum(p.realized_pnl for p in closed_pos)
        for pos in closed_pos[-5:]:
            status_emoji = {"arb": "✅", "sold": "💰", "stopped": "🛑", "expired": "⏰"}.get(pos.status.value, "❓")
            print(f"  {status_emoji} [{pos.crypto}] {pos.market_title[:40]} | P&L: ${pos.realized_pnl:+.2f}")
        print(f"\n  Total P&L: ${total_pnl:+.2f}")
    else:
        print("  None yet")
    
    print("\n" + "=" * 95)


# =============================================================================
# MAIN LOOP
# =============================================================================

async def run_bot(dry_run: bool = True, interval: int = SCAN_INTERVAL):
    """Main bot loop."""
    print("=" * 95)
    print("  CRYPTO HYBRID ARBITRAGE BOT (BTC / ETH / SOL)")
    print("=" * 95)
    print(f"  Mode: {'DRY RUN (simulated trades)' if dry_run else 'LIVE TRADING'}")
    print(f"  Cryptos: {', '.join(SUPPORTED_CRYPTOS)}")
    print(f"  Entry: Target price ${TARGET_ENTRY_PRICE:.2f}, max ask ${MAX_ENTRY_ASK:.2f}")
    print(f"  Exit 1: Sell at {PROFIT_TARGET_PCT}% profit")
    print(f"  Exit 2: Complete arb when combined <= ${MAX_COMBINED_FOR_ARB:.2f}")
    print(f"  Exit 3: Stop loss at {STOP_LOSS_PCT}%")
    print(f"  Position size: ${POSITION_SIZE_USD}")
    print("=" * 95)
    
    clob_client = None
    if not dry_run:
        clob_client = ClobClient()
        await clob_client.initialize()
        print("✓ CLOB client initialized")
    
    positions = load_positions()
    print(f"✓ Loaded {len(positions)} positions")
    
    try:
        while True:
            try:
                print(f"\n[{datetime.now().strftime('%H:%M:%S')}] Scanning...")
                
                # Fetch all crypto prices and update trends
                crypto_prices = await fetch_crypto_prices()
                update_crypto_price_history(crypto_prices)
                crypto_trends = get_all_crypto_trends()
                
                # Search and parse markets for all cryptos
                events = await search_crypto_markets()
                markets = []
                for event in events:
                    # Use lower min_minutes for monitoring existing positions
                    market = parse_market(event, min_minutes=2)
                    if market:
                        market = await enrich_market(market)
                        markets.append(market)
                
                markets.sort(key=lambda m: m.end_time)
                
                # Display status
                display_status(markets, positions, crypto_trends)
                
                # === CHECK EXISTING POSITIONS ===
                print("\n🔍 CHECKING POSITIONS...")
                for pos in positions:
                    if pos.status != PositionStatus.OPEN:
                        continue
                    
                    # Find corresponding market
                    market = None
                    for m in markets:
                        if m.market_id == pos.market_id:
                            market = m
                            break
                    
                    if not market:
                        # Market might have expired
                        end = datetime.fromisoformat(pos.end_time.replace("Z", "+00:00"))
                        if end <= datetime.now(timezone.utc):
                            pos.status = PositionStatus.EXPIRED
                            pos.exit_reason = "Market expired"
                            print(f"  ⏰ Position expired: {pos.market_title[:40]}")
                        continue
                    
                    # Check exit conditions
                    exit_action = check_exit_conditions(pos, market, crypto_trends)
                    
                    if exit_action:
                        print(f"\n  📍 {pos.market_title[:45]}")
                        print(f"     Holding: {pos.side} @ ${pos.entry_price:.4f}")
                        print(f"     Action: {exit_action['reason']}")
                        
                        if exit_action["action"] == "sell":
                            # Sell our position
                            success = await execute_sell(
                                clob_client,
                                pos.token_id,
                                exit_action["price"],
                                pos.size,
                                dry_run
                            )
                            if success or dry_run:
                                pos.status = PositionStatus.SOLD if "PROFIT" in exit_action["reason"] else PositionStatus.STOPPED
                                pos.exit_price = exit_action["price"]
                                pos.exit_time = datetime.now(timezone.utc).isoformat()
                                pos.exit_reason = exit_action["reason"]
                                pos.realized_pnl = exit_action.get("pnl", 0)
                        
                        elif exit_action["action"] == "arb":
                            # Complete the arbitrage by buying other side
                            other_amount = POSITION_SIZE_USD
                            success, size = await execute_buy(
                                clob_client,
                                pos.other_token_id,
                                exit_action["other_price"],
                                other_amount,
                                dry_run
                            )
                            if success or dry_run:
                                pos.status = PositionStatus.ARB_COMPLETE
                                pos.other_entry_price = exit_action["other_price"]
                                pos.other_size = size
                                pos.exit_time = datetime.now(timezone.utc).isoformat()
                                pos.exit_reason = exit_action["reason"]
                                # Arb profit = $1 * min_shares - total_cost
                                min_shares = min(pos.size, size)
                                total_cost = (pos.entry_price * pos.size) + (exit_action["other_price"] * size)
                                pos.realized_pnl = min_shares - total_cost
                
                # === LOOK FOR NEW ENTRIES ===
                print("\n🎯 LOOKING FOR ENTRIES...")
                for market in markets:
                    entry = check_entry_opportunity(market, positions, crypto_trends)
                    
                    if entry:
                        now = datetime.now(timezone.utc)
                        mins = (market.end_time - now).total_seconds() / 60
                        
                        trend_info = f"BTC {entry.get('trend', 'N/A')} ({entry.get('trend_strength', 'N/A')})"
                        note = entry.get('note', '')
                        
                        print(f"\n  💡 ENTRY OPPORTUNITY: {market.title[:45]}")
                        print(f"     {entry['side']} @ ${entry['price']:.4f} | {mins:.0f}min left | {trend_info}")
                        if note:
                            print(f"     Note: {note}")
                        
                        # Execute entry
                        success, size = await execute_buy(
                            clob_client,
                            entry["token_id"],
                            entry["price"],
                            POSITION_SIZE_USD,
                            dry_run
                        )
                        
                        if success or dry_run:
                            new_pos = Position(
                                id=f"{market.market_id}_{now.timestamp()}",
                                market_id=market.market_id,
                                market_title=market.title,
                                crypto=market.crypto,
                                end_time=market.end_time.isoformat(),
                                side=entry["side"],
                                token_id=entry["token_id"],
                                entry_price=entry["price"],
                                size=size,
                                entry_time=now.isoformat(),
                                other_side=entry["other_side"],
                                other_token_id=entry["other_token_id"],
                            )
                            positions.append(new_pos)
                            print(f"     ✓ {market.crypto} position opened!")
                
                # Save positions
                save_positions(positions)
                
            except Exception as e:
                print(f"Error: {e}")
                import traceback
                traceback.print_exc()
            
            print(f"\nSleeping {interval}s...")
            await asyncio.sleep(interval)
            
    finally:
        if clob_client:
            await clob_client.close()
        save_positions(positions)


async def main():
    """Entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(description="BTC Hybrid Arbitrage Bot")
    parser.add_argument("--live", action="store_true", help="Enable live trading")
    parser.add_argument("--interval", type=int, default=SCAN_INTERVAL, help="Scan interval (seconds)")
    args = parser.parse_args()
    
    dry_run = not args.live
    
    if not dry_run and not config.PRIVATE_KEY:
        print("ERROR: PRIVATE_KEY required for live trading")
        return
    
    await run_bot(dry_run=dry_run, interval=args.interval)


if __name__ == "__main__":
    asyncio.run(main())
