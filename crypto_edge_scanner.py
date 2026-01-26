#!/usr/bin/env python3
"""
Crypto Edge Scanner - Detects mispriced hourly Up/Down markets on Polymarket.

Compares live crypto prices against market probabilities to find edge opportunities.
Manages positions with profit-taking and stop-loss logic.
"""

import asyncio
import json
import os
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone, timedelta
from typing import Optional
from pathlib import Path
import math

import httpx
from dotenv import load_dotenv
load_dotenv()

from api.clob_client import ClobClient
import config

# =============================================================================
# CONFIGURATION
# =============================================================================

# Entry thresholds
ENTRY_EDGE_THRESHOLD = 0.08          # 8% edge required to enter (more selective)
MIN_MINUTES_TO_ENTER = 10            # Don't enter with less than 15 min remaining (avoid binary risk)

# Profit taking (take profits earlier)
PROFIT_TARGET_PCT = 12               # Sell at 12% profit (was 20%)
MIN_PROFIT_EDGE_EXIT = 5             # Sell at 5% profit if edge gone (was 10%)
EDGE_THRESHOLD_EXIT = 0.03           # Edge considered "gone" below 3%

# Risk management (cut losses faster)
STOP_LOSS_PCT = -8                   # Cut losses at -8% (was -15%)
EDGE_REVERSAL_THRESHOLD = -0.05      # Exit if edge flips -5% (was -10%)

# Time-based exits (critical: exit before expiry to avoid binary outcome)
EXPIRY_LOCK_PROFIT_MINUTES = 10      # Lock in profits within 10 min of expiry (was 5)
MIN_PROFIT_NEAR_EXPIRY = 3           # Need at least 3% profit to lock in (was 5%)
FORCE_EXIT_MINUTES = 5               # FORCE exit with ANY profit/loss within 5 min of expiry

# Position sizing
MAX_POSITION_USD = 50                # Max per trade
MAX_TOTAL_EXPOSURE = 200             # Max across all positions

# Volatility estimates (hourly, in percentage)
HOURLY_VOLATILITY = {
    "BTC": 0.5,   # 0.5% per hour
    "ETH": 0.7,   # 0.7% per hour
    "SOL": 1.0,   # 1.0% per hour
}

GAMMA_API_URL = "https://gamma-api.polymarket.com"
POSITIONS_FILE = "edge_positions.json"


# =============================================================================
# DATA CLASSES
# =============================================================================

@dataclass
class CryptoMarket:
    """Represents an active hourly Up/Down market."""
    event_id: str
    market_id: str
    condition_id: str
    crypto: str                      # BTC, ETH, SOL
    title: str
    end_time: datetime
    up_token_id: str
    down_token_id: str
    up_price: float                  # Current price of UP token
    down_price: float                # Current price of DOWN token
    reference_price: Optional[float] = None  # Crypto price at market open
    volume: float = 0


@dataclass
class Position:
    """Tracks an open position."""
    id: str                          # Unique identifier
    market_id: str
    token_id: str
    side: str                        # "UP" or "DOWN"
    crypto: str                      # BTC, ETH, SOL
    entry_price: float               # Price we paid per share
    size: float                      # Number of shares
    entry_time: str                  # ISO format
    market_end_time: str             # ISO format
    title: str
    reference_crypto_price: float    # Crypto price when market opened
    entry_crypto_price: float        # Crypto price when we entered
    
    def to_dict(self):
        return asdict(self)
    
    @classmethod
    def from_dict(cls, d):
        return cls(**d)


@dataclass
class EdgeOpportunity:
    """Represents a detected edge opportunity."""
    market: CryptoMarket
    fair_up_prob: float
    market_up_prob: float
    edge_up: float
    edge_down: float
    recommended_side: str
    edge_magnitude: float
    current_crypto_price: float
    reference_crypto_price: float
    minutes_remaining: float


# =============================================================================
# STATISTICAL MODEL
# =============================================================================

def normal_cdf(x: float) -> float:
    """Standard normal CDF approximation (no scipy dependency)."""
    # Using error function approximation
    a1, a2, a3, a4, a5 = 0.254829592, -0.284496736, 1.421413741, -1.453152027, 1.061405429
    p = 0.3275911
    
    sign = 1 if x >= 0 else -1
    x = abs(x) / math.sqrt(2)
    
    t = 1.0 / (1.0 + p * x)
    y = 1.0 - (((((a5 * t + a4) * t) + a3) * t + a2) * t + a1) * t * math.exp(-x * x)
    
    return 0.5 * (1.0 + sign * y)


def estimate_fair_probability(
    reference_price: float,
    current_price: float,
    minutes_remaining: float,
    hourly_volatility: float
) -> float:
    """
    Estimate fair probability that price will be UP at market close.
    
    Uses Brownian motion model: with current price above reference and
    less time for reversal, UP probability increases.
    """
    if minutes_remaining <= 0:
        return 1.0 if current_price > reference_price else 0.0
    
    if reference_price <= 0:
        return 0.5
    
    # Price change as percentage
    price_change_pct = (current_price - reference_price) / reference_price * 100
    
    # Expected volatility in remaining time (scales with sqrt of time)
    remaining_vol = hourly_volatility * math.sqrt(minutes_remaining / 60)
    
    if remaining_vol <= 0.001:
        return 1.0 if price_change_pct > 0 else 0.0
    
    # Z-score
    z_score = price_change_pct / remaining_vol
    
    # Probability using normal CDF
    fair_up_prob = normal_cdf(z_score)
    
    # Clamp to reasonable range
    return max(0.01, min(0.99, fair_up_prob))


def calculate_edge(
    market_up_price: float,
    fair_up_prob: float
) -> dict:
    """Calculate edge between fair probability and market price."""
    edge_up = fair_up_prob - market_up_price
    edge_down = -edge_up
    
    return {
        "fair_up_prob": fair_up_prob,
        "market_up_prob": market_up_price,
        "edge_up": edge_up,
        "edge_down": edge_down,
        "recommended_side": "UP" if edge_up > 0 else "DOWN",
        "edge_magnitude": abs(edge_up)
    }


# =============================================================================
# MARKET DATA FETCHING
# =============================================================================

async def fetch_live_crypto_prices() -> dict[str, float]:
    """Fetch current crypto prices from Binance API."""
    symbols = {
        "BTCUSDT": "BTC",
        "ETHUSDT": "ETH",
        "SOLUSDT": "SOL",
    }
    
    prices = {}
    
    async with httpx.AsyncClient(timeout=10) as client:
        for symbol, crypto in symbols.items():
            try:
                url = f"https://api.binance.com/api/v3/ticker/price?symbol={symbol}"
                response = await client.get(url)
                response.raise_for_status()
                data = response.json()
                prices[crypto] = float(data.get("price", 0))
            except Exception as e:
                print(f"  Warning: Failed to fetch {crypto} price: {e}")
                prices[crypto] = 0
    
    return prices


async def search_hourly_markets(crypto: str) -> list[dict]:
    """Search for active hourly Up/Down markets for a crypto."""
    # Map short names to full names for search
    name_map = {
        "BTC": "Bitcoin",
        "ETH": "Ethereum", 
        "SOL": "Solana"
    }
    
    full_name = name_map.get(crypto, crypto)
    
    # Generate date-specific search terms for upcoming days
    # The API returns old/popular markets first, so we need to search by date
    now = datetime.now(timezone.utc)
    search_terms = []
    
    for days_ahead in range(3):  # Today, tomorrow, day after
        date = now + timedelta(days=days_ahead)
        # Format: "January 27" (no leading zero on day)
        date_str = date.strftime("%B %d").replace(" 0", " ")
        search_terms.append(f"{full_name} Up or Down {date_str}")
    
    all_events = []
    
    async with httpx.AsyncClient(timeout=30) as client:
        for term in search_terms:
            try:
                response = await client.get(
                    f"{GAMMA_API_URL}/public-search",
                    params={
                        "q": term,
                        "limit_per_type": 100,
                        "search_tags": "false",
                        "search_profiles": "false",
                    }
                )
                response.raise_for_status()
                data = response.json()
                
                events = data.get("events", []) or []
                for event in events:
                    title = event.get("title", "").lower()
                    if "up or down" in title and full_name.lower() in title:
                        all_events.append(event)
                        
            except Exception as e:
                print(f"  Warning: Search failed for '{term}': {e}")
    
    # Deduplicate by event ID
    seen = set()
    unique_events = []
    for event in all_events:
        event_id = event.get("id")
        if event_id and event_id not in seen:
            seen.add(event_id)
            unique_events.append(event)
    
    return unique_events


def parse_market_to_crypto_market(event: dict, crypto: str) -> Optional[CryptoMarket]:
    """Parse a Polymarket event into our CryptoMarket structure."""
    markets = event.get("markets", [])
    if not markets:
        return None
    
    market = markets[0]  # Usually only one market per Up/Down event
    
    # Parse end time
    end_date_str = market.get("endDate") or event.get("endDate")
    if not end_date_str:
        return None
    
    try:
        end_time = datetime.fromisoformat(end_date_str.replace("Z", "+00:00"))
    except:
        return None
    
    # Check if market is still active
    now = datetime.now(timezone.utc)
    if end_time <= now:
        return None
    
    # Parse token IDs
    token_ids_str = market.get("clobTokenIds", "")
    try:
        token_ids = json.loads(token_ids_str) if token_ids_str else []
    except:
        token_ids = []
    
    if len(token_ids) < 2:
        return None
    
    # Parse outcome prices
    prices_str = market.get("outcomePrices", "")
    try:
        prices = json.loads(prices_str) if prices_str else []
        prices = [float(p) for p in prices]
    except:
        prices = [0.5, 0.5]
    
    if len(prices) < 2:
        prices = [0.5, 0.5]
    
    # Determine which token is UP vs DOWN
    # Usually: first outcome is "Up", second is "Down"
    outcomes_str = market.get("outcomes", "")
    try:
        outcomes = json.loads(outcomes_str) if outcomes_str else ["Up", "Down"]
    except:
        outcomes = ["Up", "Down"]
    
    up_idx = 0
    down_idx = 1
    for i, outcome in enumerate(outcomes):
        if "up" in str(outcome).lower():
            up_idx = i
        elif "down" in str(outcome).lower():
            down_idx = i
    
    return CryptoMarket(
        event_id=event.get("id", ""),
        market_id=market.get("id", ""),
        condition_id=market.get("conditionId", ""),
        crypto=crypto,
        title=event.get("title", market.get("question", "")),
        end_time=end_time,
        up_token_id=token_ids[up_idx] if len(token_ids) > up_idx else "",
        down_token_id=token_ids[down_idx] if len(token_ids) > down_idx else "",
        up_price=prices[up_idx] if len(prices) > up_idx else 0.5,
        down_price=prices[down_idx] if len(prices) > down_idx else 0.5,
        volume=float(event.get("volume", 0) or 0),
    )


async def get_active_hourly_markets() -> list[CryptoMarket]:
    """Get all active hourly Up/Down markets for BTC, ETH, SOL."""
    all_markets = []
    
    for crypto in ["BTC", "ETH", "SOL"]:
        print(f"  Searching {crypto} markets...")
        events = await search_hourly_markets(crypto)
        
        for event in events:
            market = parse_market_to_crypto_market(event, crypto)
            if market:
                all_markets.append(market)
    
    # Sort by end time (soonest first)
    all_markets.sort(key=lambda m: m.end_time)
    
    return all_markets


# =============================================================================
# POSITION MANAGEMENT
# =============================================================================

def load_positions() -> list[Position]:
    """Load positions from file."""
    if not Path(POSITIONS_FILE).exists():
        return []
    
    try:
        with open(POSITIONS_FILE, "r") as f:
            data = json.load(f)
            return [Position.from_dict(p) for p in data]
    except Exception as e:
        print(f"Warning: Could not load positions: {e}")
        return []


def save_positions(positions: list[Position]):
    """Save positions to file."""
    with open(POSITIONS_FILE, "w") as f:
        json.dump([p.to_dict() for p in positions], f, indent=2)


def should_take_profit(
    position: Position,
    current_token_price: float,
    current_fair_prob: float,
    minutes_remaining: float
) -> dict:
    """Decide whether to sell position for profit."""
    entry_price = position.entry_price
    
    if entry_price <= 0:
        return {"should_sell": False, "reason": "Invalid entry price", "urgency": None}
    
    profit_pct = (current_token_price - entry_price) / entry_price * 100
    
    # Calculate current edge from our perspective
    if position.side == "UP":
        our_fair_value = current_fair_prob
    else:
        our_fair_value = 1 - current_fair_prob
    
    current_edge = our_fair_value - current_token_price
    
    # === SELL CONDITIONS (ordered by priority) ===
    
    # 0. FORCE EXIT NEAR EXPIRY - avoid binary outcome at all costs
    if minutes_remaining <= FORCE_EXIT_MINUTES:
        return {
            "should_sell": True,
            "reason": f"FORCE_EXIT: {profit_pct:+.1f}%, only {minutes_remaining:.0f}min left",
            "urgency": "critical"
        }
    
    # 1. PROFIT TARGET HIT
    if profit_pct >= PROFIT_TARGET_PCT:
        return {
            "should_sell": True,
            "reason": f"PROFIT_TARGET: +{profit_pct:.1f}% gain",
            "urgency": "high"
        }
    
    # 2. STOP LOSS - cut losses quickly
    if profit_pct <= STOP_LOSS_PCT:
        return {
            "should_sell": True,
            "reason": f"STOP_LOSS: {profit_pct:.1f}% loss",
            "urgency": "high"
        }
    
    # 3. GOOD PROFIT + EDGE GONE
    if profit_pct >= MIN_PROFIT_EDGE_EXIT and current_edge < EDGE_THRESHOLD_EXIT:
        return {
            "should_sell": True,
            "reason": f"EDGE_EVAPORATED: +{profit_pct:.1f}% gain, edge {current_edge:.1%}",
            "urgency": "medium"
        }
    
    # 4. NEAR EXPIRY + IN PROFIT (wider window)
    if minutes_remaining <= EXPIRY_LOCK_PROFIT_MINUTES and profit_pct >= MIN_PROFIT_NEAR_EXPIRY:
        return {
            "should_sell": True,
            "reason": f"NEAR_EXPIRY: +{profit_pct:.1f}% gain, {minutes_remaining:.0f}min left",
            "urgency": "high"
        }
    
    # 5. EDGE REVERSED SIGNIFICANTLY (exit even if small loss)
    if current_edge <= EDGE_REVERSAL_THRESHOLD and profit_pct < 2:
        return {
            "should_sell": True,
            "reason": f"EDGE_REVERSED: edge {current_edge:.1%}, P&L {profit_pct:+.1f}%",
            "urgency": "medium"
        }
    
    return {
        "should_sell": False,
        "reason": f"HOLD: profit {profit_pct:+.1f}%, edge {current_edge:.1%}",
        "urgency": None
    }


# =============================================================================
# REFERENCE PRICE ESTIMATION
# =============================================================================

def estimate_reference_price(
    market: CryptoMarket,
    current_price: float,
    current_up_prob: float
) -> float:
    """
    Estimate the reference price (market open price) from current market state.
    
    If market is at 50/50, reference price ≈ current price.
    If market is at 60/40 UP, price has moved up from reference.
    
    This is an approximation - ideally we'd get this from Polymarket directly.
    """
    # Time remaining
    now = datetime.now(timezone.utc)
    minutes_remaining = max(0, (market.end_time - now).total_seconds() / 60)
    
    if minutes_remaining <= 0:
        return current_price
    
    # If market is 50/50, current price IS the reference
    if abs(current_up_prob - 0.5) < 0.02:
        return current_price
    
    # Estimate how much price has moved based on market probability
    hourly_vol = HOURLY_VOLATILITY.get(market.crypto, 0.5)
    remaining_vol = hourly_vol * math.sqrt(minutes_remaining / 60)
    
    if remaining_vol <= 0.001:
        return current_price
    
    # Inverse normal CDF approximation
    # If P(up) = 0.7, then z ≈ 0.52, meaning price is 0.52 * vol above reference
    from math import log, sqrt
    
    # Simple approximation of inverse normal CDF
    p = current_up_prob
    if p <= 0.01:
        z_score = -2.33
    elif p >= 0.99:
        z_score = 2.33
    else:
        # Rational approximation
        if p < 0.5:
            t = sqrt(-2 * log(p))
            z_score = -(t - (2.515517 + 0.802853*t + 0.010328*t*t) / 
                        (1 + 1.432788*t + 0.189269*t*t + 0.001308*t*t*t))
        else:
            t = sqrt(-2 * log(1 - p))
            z_score = t - (2.515517 + 0.802853*t + 0.010328*t*t) / \
                      (1 + 1.432788*t + 0.189269*t*t + 0.001308*t*t*t)
    
    # Price change from reference = z_score * remaining_vol
    price_change_pct = z_score * remaining_vol
    
    # Reference = current / (1 + change%)
    reference = current_price / (1 + price_change_pct / 100)
    
    return reference


# =============================================================================
# MAIN SCANNER
# =============================================================================

async def scan_for_opportunities(
    markets: list[CryptoMarket],
    crypto_prices: dict[str, float],
    positions: list[Position]
) -> list[EdgeOpportunity]:
    """Scan markets for edge opportunities."""
    opportunities = []
    now = datetime.now(timezone.utc)
    
    # Get position market IDs to skip
    position_market_ids = {p.market_id for p in positions}
    
    for market in markets:
        # Skip if we already have a position
        if market.market_id in position_market_ids:
            continue
        
        # Skip if not enough time remaining
        minutes_remaining = (market.end_time - now).total_seconds() / 60
        if minutes_remaining < MIN_MINUTES_TO_ENTER:
            continue
        
        current_price = crypto_prices.get(market.crypto, 0)
        if current_price <= 0:
            continue
        
        # Estimate reference price
        reference_price = estimate_reference_price(
            market, current_price, market.up_price
        )
        
        # Calculate fair probability
        hourly_vol = HOURLY_VOLATILITY.get(market.crypto, 0.5)
        fair_up_prob = estimate_fair_probability(
            reference_price, current_price, minutes_remaining, hourly_vol
        )
        
        # Calculate edge
        edge = calculate_edge(market.up_price, fair_up_prob)
        
        # Check if edge exceeds threshold
        if edge["edge_magnitude"] >= ENTRY_EDGE_THRESHOLD:
            opportunities.append(EdgeOpportunity(
                market=market,
                fair_up_prob=fair_up_prob,
                market_up_prob=market.up_price,
                edge_up=edge["edge_up"],
                edge_down=edge["edge_down"],
                recommended_side=edge["recommended_side"],
                edge_magnitude=edge["edge_magnitude"],
                current_crypto_price=current_price,
                reference_crypto_price=reference_price,
                minutes_remaining=minutes_remaining,
            ))
    
    # Sort by edge magnitude (best first)
    opportunities.sort(key=lambda o: o.edge_magnitude, reverse=True)
    
    return opportunities


async def execute_buy(
    clob_client: ClobClient,
    market: CryptoMarket,
    side: str,
    crypto_prices: dict[str, float],
    reference_price: float,
    dry_run: bool = True
) -> Optional[Position]:
    """Execute a buy order for an opportunity."""
    token_id = market.up_token_id if side == "UP" else market.down_token_id
    current_price = market.up_price if side == "UP" else market.down_price
    
    # Calculate size
    size = MAX_POSITION_USD / current_price
    
    print(f"\n{'[DRY RUN] ' if dry_run else ''}BUYING {side} on {market.crypto}")
    print(f"  Market: {market.title}")
    print(f"  Token: {token_id[:20]}...")
    print(f"  Price: ${current_price:.4f}")
    print(f"  Size: {size:.2f} shares (${MAX_POSITION_USD:.2f})")
    
    if not dry_run:
        try:
            result = await clob_client.place_order(
                token_id=token_id,
                side="BUY",
                price=current_price,
                size=size,
            )
            
            if not result.success:
                print(f"  ✗ Order failed: {result.error}")
                return None
            
            print(f"  ✓ Order placed: {result.order_id}")
            
        except Exception as e:
            print(f"  ✗ Error: {e}")
            return None
    
    # Create position record
    position = Position(
        id=f"{market.market_id}_{side}_{datetime.now().timestamp()}",
        market_id=market.market_id,
        token_id=token_id,
        side=side,
        crypto=market.crypto,
        entry_price=current_price,
        size=size,
        entry_time=datetime.now(timezone.utc).isoformat(),
        market_end_time=market.end_time.isoformat(),
        title=market.title,
        reference_crypto_price=reference_price,
        entry_crypto_price=crypto_prices.get(market.crypto, 0),
    )
    
    return position


async def execute_sell(
    clob_client: ClobClient,
    position: Position,
    current_price: float,
    reason: str,
    dry_run: bool = True
) -> bool:
    """Execute a sell order to close a position."""
    profit_pct = (current_price - position.entry_price) / position.entry_price * 100
    profit_usd = (current_price - position.entry_price) * position.size
    
    print(f"\n{'[DRY RUN] ' if dry_run else ''}SELLING {position.side} on {position.crypto}")
    print(f"  Reason: {reason}")
    print(f"  Entry: ${position.entry_price:.4f} -> Current: ${current_price:.4f}")
    print(f"  P&L: {profit_pct:+.1f}% (${profit_usd:+.2f})")
    
    if not dry_run:
        try:
            result = await clob_client.place_order(
                token_id=position.token_id,
                side="SELL",
                price=current_price,
                size=position.size,
            )
            
            if not result.success:
                print(f"  ✗ Order failed: {result.error}")
                return False
            
            print(f"  ✓ Order placed: {result.order_id}")
            
        except Exception as e:
            print(f"  ✗ Error: {e}")
            return False
    
    return True


async def get_token_price(clob_client: ClobClient, token_id: str) -> Optional[float]:
    """Get current best bid price for a token."""
    try:
        orderbook = await clob_client.get_orderbook(token_id)
        return orderbook.best_bid
    except:
        return None


# =============================================================================
# DISPLAY
# =============================================================================

def display_status(
    markets: list[CryptoMarket],
    opportunities: list[EdgeOpportunity],
    positions: list[Position],
    crypto_prices: dict[str, float]
):
    """Display current scanner status."""
    now = datetime.now(timezone.utc)
    
    print("\n" + "=" * 80)
    print(f"  CRYPTO EDGE SCANNER - {now.strftime('%Y-%m-%d %H:%M:%S')} UTC")
    print("=" * 80)
    
    # Crypto prices
    print("\n📊 LIVE PRICES")
    print("-" * 80)
    for crypto, price in crypto_prices.items():
        print(f"  {crypto}: ${price:,.2f}")
    
    # Active markets summary
    print(f"\n📈 ACTIVE MARKETS ({len(markets)} found)")
    print("-" * 80)
    
    by_crypto = {}
    for m in markets:
        by_crypto.setdefault(m.crypto, []).append(m)
    
    for crypto, ms in by_crypto.items():
        active = [m for m in ms if m.end_time > now]
        print(f"  {crypto}: {len(active)} active markets")
    
    # Opportunities
    print(f"\n🎯 EDGE OPPORTUNITIES ({len(opportunities)})")
    print("-" * 80)
    
    if opportunities:
        print(f"{'Crypto':<6} {'Side':<6} {'Edge':>8} {'Fair':>8} {'Market':>8} {'Mins':>6}")
        print("-" * 80)
        for opp in opportunities[:10]:
            print(f"{opp.market.crypto:<6} {opp.recommended_side:<6} "
                  f"{opp.edge_magnitude:>7.1%} {opp.fair_up_prob:>7.1%} "
                  f"{opp.market_up_prob:>7.1%} {opp.minutes_remaining:>5.0f}")
    else:
        print("  No opportunities above threshold")
    
    # Positions
    print(f"\n💼 OPEN POSITIONS ({len(positions)})")
    print("-" * 80)
    
    if positions:
        total_pnl = 0
        print(f"{'Crypto':<6} {'Side':<6} {'Entry':>8} {'Current':>8} {'P&L':>10} {'Mins':>6}")
        print("-" * 80)
        for pos in positions:
            end_time = datetime.fromisoformat(pos.market_end_time.replace("Z", "+00:00"))
            mins_left = max(0, (end_time - now).total_seconds() / 60)
            
            # Estimate current price (would need real-time data)
            current = pos.entry_price  # Placeholder
            pnl_pct = 0
            
            print(f"{pos.crypto:<6} {pos.side:<6} "
                  f"${pos.entry_price:>7.4f} ${current:>7.4f} "
                  f"{pnl_pct:>9.1f}% {mins_left:>5.0f}")
    else:
        print("  No open positions")
    
    print("\n" + "=" * 80)


# =============================================================================
# MAIN LOOP
# =============================================================================

async def run_scanner(dry_run: bool = True, interval_seconds: int = 60):
    """Main scanner loop."""
    print("=" * 80)
    print("  CRYPTO EDGE SCANNER")
    print(f"  Mode: {'DRY RUN' if dry_run else 'LIVE TRADING'}")
    print(f"  Interval: {interval_seconds}s")
    print("=" * 80)
    
    # Initialize CLOB client
    clob_client = None
    if not dry_run:
        clob_client = ClobClient()
        await clob_client.initialize()
        print("✓ CLOB client initialized")
    
    positions = load_positions()
    print(f"✓ Loaded {len(positions)} existing positions")
    
    try:
        while True:
            try:
                print(f"\n[{datetime.now().strftime('%H:%M:%S')}] Scanning...")
                
                # Fetch data
                print("  Fetching crypto prices...")
                crypto_prices = await fetch_live_crypto_prices()
                
                print("  Searching for active markets...")
                markets = await get_active_hourly_markets()
                
                # Scan for opportunities
                opportunities = await scan_for_opportunities(
                    markets, crypto_prices, positions
                )
                
                # Display status
                display_status(markets, opportunities, positions, crypto_prices)
                
                # === POSITION MANAGEMENT ===
                now = datetime.now(timezone.utc)
                positions_to_remove = []
                
                for position in positions:
                    # Check if market has ended
                    end_time = datetime.fromisoformat(
                        position.market_end_time.replace("Z", "+00:00")
                    )
                    if end_time <= now:
                        print(f"\n  Position expired: {position.crypto} {position.side}")
                        positions_to_remove.append(position)
                        continue
                    
                    # Get current token price
                    current_price = None
                    if clob_client:
                        current_price = await get_token_price(
                            clob_client, position.token_id
                        )
                    
                    if current_price is None:
                        continue
                    
                    # Calculate fair probability
                    minutes_remaining = (end_time - now).total_seconds() / 60
                    current_crypto = crypto_prices.get(position.crypto, 0)
                    hourly_vol = HOURLY_VOLATILITY.get(position.crypto, 0.5)
                    
                    fair_prob = estimate_fair_probability(
                        position.reference_crypto_price,
                        current_crypto,
                        minutes_remaining,
                        hourly_vol
                    )
                    
                    # Check if should exit
                    exit_decision = should_take_profit(
                        position, current_price, fair_prob, minutes_remaining
                    )
                    
                    if exit_decision["should_sell"]:
                        success = await execute_sell(
                            clob_client, position, current_price,
                            exit_decision["reason"], dry_run
                        )
                        if success or dry_run:
                            positions_to_remove.append(position)
                
                # Remove closed positions
                for pos in positions_to_remove:
                    if pos in positions:
                        positions.remove(pos)
                
                # === ENTRY LOGIC ===
                total_exposure = sum(p.entry_price * p.size for p in positions)
                
                for opp in opportunities:
                    # Check exposure limit
                    if total_exposure + MAX_POSITION_USD > MAX_TOTAL_EXPOSURE:
                        print(f"\n  Max exposure reached (${total_exposure:.2f})")
                        break
                    
                    # Execute buy
                    new_position = await execute_buy(
                        clob_client, opp.market, opp.recommended_side,
                        crypto_prices, opp.reference_crypto_price, dry_run
                    )
                    
                    if new_position:
                        positions.append(new_position)
                        total_exposure += MAX_POSITION_USD
                
                # Save positions
                save_positions(positions)
                
            except Exception as e:
                print(f"Error in scan loop: {e}")
                import traceback
                traceback.print_exc()
            
            # Wait for next iteration
            print(f"\nSleeping {interval_seconds}s...")
            await asyncio.sleep(interval_seconds)
            
    finally:
        if clob_client:
            await clob_client.close()


async def main():
    """Entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Crypto Edge Scanner")
    parser.add_argument("--live", action="store_true", help="Enable live trading")
    parser.add_argument("--interval", type=int, default=60, help="Scan interval in seconds")
    args = parser.parse_args()
    
    dry_run = not args.live
    
    if not dry_run and not config.PRIVATE_KEY:
        print("ERROR: PRIVATE_KEY required for live trading")
        return
    
    await run_scanner(dry_run=dry_run, interval_seconds=args.interval)


if __name__ == "__main__":
    asyncio.run(main())
