#!/usr/bin/env python3
"""
BTC Up/Down Arbitrage Bot

Strategy: Buy both sides of a market when combined cost < $1
- Place limit orders at good prices
- Wait for volatility to fill orders
- When both sides filled for combined < $1 = guaranteed profit

Based on: https://polymarket.com/@nobuyoshi005
"""

import asyncio
import json
from dataclasses import dataclass, asdict
from datetime import datetime, timezone, timedelta
from typing import Optional
from pathlib import Path

import httpx
from dotenv import load_dotenv
load_dotenv()

from api.clob_client import ClobClient
import config

# =============================================================================
# CONFIGURATION
# =============================================================================

# Target profit margin (e.g., 0.05 = 5% minimum profit)
MIN_PROFIT_MARGIN = 0.05  # 5% minimum guaranteed profit

# Price limits for placing orders
MAX_PRICE_FIRST_SIDE = 0.40   # Max price to bid for first side (40c)
MAX_COMBINED_PRICE = 0.95     # Max combined price (95c for 5% profit)

# Position sizing
POSITION_SIZE_USD = 50  # USD per side

# How often to scan (seconds)
SCAN_INTERVAL = 15

# Market time window
MIN_MINUTES_TO_EXPIRY = 5
MAX_MINUTES_TO_EXPIRY = 60  # Focus on 15min to 1hr markets

GAMMA_API_URL = "https://gamma-api.polymarket.com"
CLOB_API_URL = "https://clob.polymarket.com"
POSITIONS_FILE = "arbitrage_positions.json"


# =============================================================================
# DATA CLASSES
# =============================================================================

@dataclass
class BTCMarket:
    """A BTC Up/Down market."""
    event_id: str
    market_id: str
    title: str
    end_time: datetime
    up_token_id: str
    down_token_id: str
    # Orderbook data
    up_best_ask: Optional[float] = None
    up_best_bid: Optional[float] = None
    down_best_ask: Optional[float] = None
    down_best_bid: Optional[float] = None
    # Current market mid prices
    up_mid: float = 0.5
    down_mid: float = 0.5


@dataclass
class ArbitragePosition:
    """Tracks a position in an arbitrage."""
    id: str
    market_id: str
    title: str
    end_time: str
    
    # First side
    first_side: str  # "UP" or "DOWN"
    first_token_id: str
    first_order_price: float  # Price we placed limit order at
    first_filled: bool = False
    first_fill_price: Optional[float] = None
    first_size: float = 0
    first_time: Optional[str] = None
    
    # Second side
    second_side: Optional[str] = None
    second_token_id: Optional[str] = None
    second_order_price: Optional[float] = None
    second_filled: bool = False
    second_fill_price: Optional[float] = None
    second_size: float = 0
    second_time: Optional[str] = None
    
    # Status
    status: str = "pending_first"  # pending_first, pending_second, complete, expired
    total_cost: float = 0
    guaranteed_profit: float = 0
    
    def to_dict(self):
        return asdict(self)
    
    @classmethod
    def from_dict(cls, d):
        return cls(**d)


# =============================================================================
# SEARCH FUNCTIONS
# =============================================================================

def get_search_terms():
    """Generate search terms with current date."""
    now = datetime.now()
    terms = []
    for day_offset in range(0, 2):
        d = now + timedelta(days=day_offset)
        month = d.strftime("%B")
        day = d.day
        terms.append(f"Bitcoin Up or Down {month} {day}")
    return terms


async def search_btc_markets() -> list[dict]:
    """Search for active BTC Up/Down markets."""
    all_events = []
    search_terms = get_search_terms()
    
    async with httpx.AsyncClient(timeout=30) as client:
        for term in search_terms:
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
                
                events = data.get("events", []) or []
                for event in events:
                    title = event.get("title", "").lower()
                    if ("bitcoin" in title or "btc" in title) and "up or down" in title:
                        all_events.append(event)
                        
            except Exception as e:
                print(f"  Warning: Search failed for '{term}': {e}")
    
    # Deduplicate
    seen = set()
    unique = []
    for event in all_events:
        eid = event.get("id")
        if eid and eid not in seen:
            seen.add(eid)
            unique.append(event)
    
    return unique


def parse_market(event: dict) -> Optional[BTCMarket]:
    """Parse event into BTCMarket."""
    markets = event.get("markets", [])
    if not markets:
        return None
    
    market = markets[0]
    
    # Parse end time
    end_date_str = market.get("endDate") or event.get("endDate")
    if not end_date_str:
        return None
    
    try:
        end_time = datetime.fromisoformat(end_date_str.replace("Z", "+00:00"))
    except:
        return None
    
    # Check time window
    now = datetime.now(timezone.utc)
    mins_to_expiry = (end_time - now).total_seconds() / 60
    
    if mins_to_expiry < MIN_MINUTES_TO_EXPIRY or mins_to_expiry > MAX_MINUTES_TO_EXPIRY:
        return None
    
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
    
    # Parse mid prices
    prices_str = market.get("outcomePrices", "")
    try:
        prices = json.loads(prices_str) if prices_str else []
        prices = [float(p) for p in prices]
    except:
        prices = [0.5, 0.5]
    
    return BTCMarket(
        event_id=event.get("id", ""),
        market_id=market.get("id", ""),
        title=event.get("title", ""),
        end_time=end_time,
        up_token_id=token_ids[0],
        down_token_id=token_ids[1],
        up_mid=prices[0] if prices else 0.5,
        down_mid=prices[1] if len(prices) > 1 else 0.5,
    )


async def fetch_orderbook(token_id: str) -> tuple[Optional[float], Optional[float]]:
    """Fetch best bid/ask for a token."""
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


async def enrich_market(market: BTCMarket) -> BTCMarket:
    """Fetch orderbook data for a market."""
    market.up_best_ask, market.up_best_bid = await fetch_orderbook(market.up_token_id)
    market.down_best_ask, market.down_best_bid = await fetch_orderbook(market.down_token_id)
    return market


# =============================================================================
# POSITION MANAGEMENT
# =============================================================================

def load_positions() -> list[ArbitragePosition]:
    """Load positions from file."""
    if not Path(POSITIONS_FILE).exists():
        return []
    
    try:
        with open(POSITIONS_FILE, "r") as f:
            data = json.load(f)
            return [ArbitragePosition.from_dict(p) for p in data]
    except:
        return []


def save_positions(positions: list[ArbitragePosition]):
    """Save positions to file."""
    with open(POSITIONS_FILE, "w") as f:
        json.dump([p.to_dict() for p in positions], f, indent=2)


# =============================================================================
# TRADING LOGIC
# =============================================================================

async def place_limit_order(
    clob_client: ClobClient,
    token_id: str,
    price: float,
    size: float,
    dry_run: bool = True
) -> bool:
    """Place a limit buy order."""
    print(f"    {'[DRY RUN] ' if dry_run else ''}Placing limit BUY @ ${price:.4f} for {size:.2f} shares")
    
    if not dry_run:
        try:
            result = await clob_client.place_order(
                token_id=token_id,
                side="BUY",
                price=price,
                size=size,
            )
            if result.success:
                print(f"      ✓ Order placed: {result.order_id}")
                return True
            else:
                print(f"      ✗ Order failed: {result.error}")
                return False
        except Exception as e:
            print(f"      ✗ Error: {e}")
            return False
    
    return True


def find_arbitrage_opportunity(market: BTCMarket) -> Optional[dict]:
    """
    Check if there's an arbitrage opportunity.
    
    Strategy: Look for prices where we can buy both sides for < $1
    """
    now = datetime.now(timezone.utc)
    mins_left = (market.end_time - now).total_seconds() / 60
    
    # Need orderbook data
    if market.up_best_bid is None or market.down_best_bid is None:
        return None
    
    # The key insight: we place limit orders at good prices
    # If we can get UP at X and DOWN at Y where X + Y < $1, we profit
    
    # Look at current best bids to see what prices are possible
    up_price = market.up_best_bid
    down_price = market.down_best_bid
    
    # Also consider mid prices for potential fills
    up_target = min(market.up_mid, MAX_PRICE_FIRST_SIDE)
    down_target = min(market.down_mid, MAX_PRICE_FIRST_SIDE)
    
    # Check if we could potentially get both sides cheap enough
    # Conservative: use current bids as what we might get
    combined = up_price + down_price
    
    if combined < MAX_COMBINED_PRICE:
        profit = 1.0 - combined
        return {
            "market": market,
            "up_price": up_price,
            "down_price": down_price,
            "combined": combined,
            "profit": profit,
            "profit_pct": profit / combined * 100,
            "mins_left": mins_left,
        }
    
    # Check if either side is very cheap (opportunity to start position)
    cheaper_side = "UP" if up_price < down_price else "DOWN"
    cheaper_price = min(up_price, down_price)
    other_price = max(up_price, down_price)
    
    # If one side is cheap enough, we could start a position
    if cheaper_price <= MAX_PRICE_FIRST_SIDE:
        # Estimate if we could complete the arb
        # If we get first side at cheaper_price, we need other side at < (MAX_COMBINED - cheaper_price)
        max_other = MAX_COMBINED_PRICE - cheaper_price
        
        if max_other >= 0.3:  # Reasonable chance other side could swing to this
            return {
                "market": market,
                "strategy": "staged",
                "first_side": cheaper_side,
                "first_price": cheaper_price,
                "needed_other_price": max_other,
                "current_other_price": other_price,
                "mins_left": mins_left,
            }
    
    return None


# =============================================================================
# DISPLAY
# =============================================================================

def display_status(markets: list[BTCMarket], positions: list[ArbitragePosition]):
    """Display current status."""
    now = datetime.now(timezone.utc)
    
    print("\n" + "=" * 80)
    print(f"  BTC ARBITRAGE BOT - {now.strftime('%Y-%m-%d %H:%M:%S')} UTC")
    print("=" * 80)
    
    # Active markets
    print(f"\n📈 ACTIVE BTC MARKETS ({len(markets)})")
    print("-" * 80)
    
    if markets:
        print(f"{'Title':<40} {'Mins':>6} {'UP Bid':>8} {'DN Bid':>8} {'Comb':>8}")
        print("-" * 80)
        for m in markets[:15]:
            mins = (m.end_time - now).total_seconds() / 60
            title = m.title[:37] + "..." if len(m.title) > 40 else m.title
            up_bid = f"${m.up_best_bid:.2f}" if m.up_best_bid else "N/A"
            dn_bid = f"${m.down_best_bid:.2f}" if m.down_best_bid else "N/A"
            
            combined = "N/A"
            if m.up_best_bid and m.down_best_bid:
                comb = m.up_best_bid + m.down_best_bid
                combined = f"${comb:.2f}"
            
            print(f"{title:<40} {mins:>5.0f}m {up_bid:>8} {dn_bid:>8} {combined:>8}")
    else:
        print("  No active markets in time window")
    
    # Positions
    pending = [p for p in positions if p.status.startswith("pending")]
    complete = [p for p in positions if p.status == "complete"]
    
    print(f"\n⏳ PENDING POSITIONS ({len(pending)})")
    print("-" * 80)
    if pending:
        for p in pending:
            print(f"  {p.title[:50]}")
            print(f"    Status: {p.status} | First: {p.first_side} @ ${p.first_order_price:.4f}")
    else:
        print("  None")
    
    print(f"\n✅ COMPLETED ARBITRAGES ({len(complete)})")
    print("-" * 80)
    if complete:
        total_profit = sum(p.guaranteed_profit for p in complete)
        for p in complete[-5:]:
            print(f"  {p.title[:50]}")
            print(f"    Cost: ${p.total_cost:.4f} | Profit: ${p.guaranteed_profit:.4f}")
        print(f"\n  Total profit: ${total_profit:.2f}")
    else:
        print("  None yet")
    
    print("\n" + "=" * 80)


# =============================================================================
# MAIN LOOP
# =============================================================================

async def run_bot(dry_run: bool = True, interval: int = SCAN_INTERVAL):
    """Main bot loop."""
    print("=" * 80)
    print("  BTC UP/DOWN ARBITRAGE BOT")
    print(f"  Mode: {'DRY RUN' if dry_run else 'LIVE TRADING'}")
    print(f"  Max first side price: ${MAX_PRICE_FIRST_SIDE:.2f}")
    print(f"  Max combined price: ${MAX_COMBINED_PRICE:.2f}")
    print(f"  Scan interval: {interval}s")
    print("=" * 80)
    
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
                
                # Search for markets
                events = await search_btc_markets()
                
                # Parse and enrich with orderbook data
                markets = []
                for event in events:
                    market = parse_market(event)
                    if market:
                        market = await enrich_market(market)
                        markets.append(market)
                
                # Sort by time to expiry
                markets.sort(key=lambda m: m.end_time)
                
                # Display status
                display_status(markets, positions)
                
                # Look for opportunities
                print("\n🔍 SCANNING FOR OPPORTUNITIES...")
                for market in markets:
                    opp = find_arbitrage_opportunity(market)
                    if opp:
                        print(f"\n  Found opportunity in: {market.title[:50]}")
                        if "strategy" in opp and opp["strategy"] == "staged":
                            print(f"    Strategy: Buy {opp['first_side']} first @ ${opp['first_price']:.4f}")
                            print(f"    Need other side < ${opp['needed_other_price']:.4f}")
                            print(f"    Current other price: ${opp['current_other_price']:.4f}")
                        else:
                            print(f"    UP: ${opp['up_price']:.4f} + DOWN: ${opp['down_price']:.4f} = ${opp['combined']:.4f}")
                            print(f"    Profit: ${opp['profit']:.4f} ({opp['profit_pct']:.1f}%)")
                
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


async def main():
    """Entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(description="BTC Arbitrage Bot")
    parser.add_argument("--live", action="store_true", help="Enable live trading")
    parser.add_argument("--interval", type=int, default=SCAN_INTERVAL, help="Scan interval")
    args = parser.parse_args()
    
    dry_run = not args.live
    
    if not dry_run and not config.PRIVATE_KEY:
        print("ERROR: PRIVATE_KEY required for live trading")
        return
    
    await run_bot(dry_run=dry_run, interval=args.interval)


if __name__ == "__main__":
    asyncio.run(main())
