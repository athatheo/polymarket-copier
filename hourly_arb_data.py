"""
Hourly Crypto Arbitrage - Data Verification Script

Fetches and displays data for the NEXT HOUR's BTC, ETH, SOL Up/Down markets.

API Endpoint: https://gamma-api.polymarket.com/events/slug/{slug}
Slug format: {asset}-up-or-down-{month}-{day}-{hour}am-et

NOTE: We use Gamma API market data (bestBid, bestAsk, outcomePrices) which are accurate.
The CLOB /book endpoint returns far OTM orders, not the actual best prices.
"""

import asyncio
import httpx
from datetime import datetime, timezone, timedelta
from dataclasses import dataclass
from typing import Optional
import json

# Polymarket API endpoints
GAMMA_API_URL = "https://gamma-api.polymarket.com"
CLOB_API_URL = "https://clob.polymarket.com"

# Assets we're tracking
ASSETS = ["bitcoin", "ethereum", "solana"]
ASSET_SHORT = {"bitcoin": "BTC", "ethereum": "ETH", "solana": "SOL"}


@dataclass
class MarketOutcome:
    """Single outcome (Up/Down) in a market."""
    token_id: str
    outcome: str  # "Up" or "Down"
    price: float  # Current mid price from API


@dataclass
class HourlyMarket:
    """Represents an hourly crypto market."""
    condition_id: str
    question: str
    slug: str
    asset: str  # BTC, ETH, SOL
    end_time: datetime
    event_start_time: Optional[datetime]
    outcomes: list[MarketOutcome]
    # Market-level prices (from Gamma API)
    best_bid: Optional[float]  # Best bid for primary outcome (Up)
    best_ask: Optional[float]  # Best ask for primary outcome (Up)
    spread: Optional[float]
    last_trade_price: Optional[float]
    volume: float
    liquidity: float


def get_current_hour_et() -> datetime:
    """Get the current hour in Eastern Time (UTC-5)."""
    utc_now = datetime.now(timezone.utc)
    et_offset = timedelta(hours=-5)
    et_now = utc_now + et_offset
    return et_now


def generate_event_slug(asset: str, target_time: datetime) -> str:
    """
    Generate event slug for hourly market.
    
    Format: {asset}-up-or-down-{month}-{day}-{hour}am-et
    Example: bitcoin-up-or-down-january-27-8am-et
    """
    months = [
        "january", "february", "march", "april", "may", "june",
        "july", "august", "september", "october", "november", "december"
    ]
    
    month_name = months[target_time.month - 1]
    day = target_time.day
    hour = target_time.hour
    
    # Convert 24h to 12h format with am/pm
    if hour == 0:
        hour_str = "12am"
    elif hour < 12:
        hour_str = f"{hour}am"
    elif hour == 12:
        hour_str = "12pm"
    else:
        hour_str = f"{hour - 12}pm"
    
    return f"{asset}-up-or-down-{month_name}-{day}-{hour_str}-et"


async def fetch_event_by_slug(http: httpx.AsyncClient, slug: str) -> Optional[dict]:
    """Fetch an event by its slug using the correct endpoint."""
    try:
        url = f"{GAMMA_API_URL}/events/slug/{slug}"
        response = await http.get(url)
        if response.status_code == 200:
            return response.json()
        return None
    except Exception as e:
        print(f"Error fetching {slug}: {e}")
        return None


async def get_market_data(http: httpx.AsyncClient, event: dict) -> Optional[HourlyMarket]:
    """Extract market data from an event using Gamma API data."""
    markets = event.get("markets", [])
    if not markets:
        return None
    
    market = markets[0]
    
    condition_id = market.get("conditionId", "")
    question = market.get("question", "")
    slug = event.get("slug", "")
    
    # Parse CLOB token IDs (stored as JSON string)
    clob_token_ids_str = market.get("clobTokenIds", "[]")
    try:
        clob_token_ids = json.loads(clob_token_ids_str) if isinstance(clob_token_ids_str, str) else clob_token_ids_str
    except:
        clob_token_ids = []
    
    # Parse outcomes (stored as JSON string)
    outcomes_str = market.get("outcomes", "[]")
    try:
        outcomes = json.loads(outcomes_str) if isinstance(outcomes_str, str) else outcomes_str
    except:
        outcomes = []
    
    # Parse outcome prices (mid prices)
    prices_str = market.get("outcomePrices", "[]")
    try:
        prices = json.loads(prices_str) if isinstance(prices_str, str) else prices_str
        prices = [float(p) for p in prices]
    except:
        prices = []
    
    if len(clob_token_ids) < 2 or len(outcomes) < 2:
        return None
    
    # Build outcome objects
    market_outcomes = []
    for idx, token_id in enumerate(clob_token_ids):
        outcome_name = outcomes[idx] if idx < len(outcomes) else f"Outcome{idx}"
        price = prices[idx] if idx < len(prices) else 0.0
        
        market_outcomes.append(MarketOutcome(
            token_id=token_id,
            outcome=outcome_name,
            price=price,
        ))
    
    # Parse end time
    end_str = market.get("endDate")
    end_time = None
    if end_str:
        try:
            if end_str.endswith("Z"):
                end_str = end_str[:-1] + "+00:00"
            end_time = datetime.fromisoformat(end_str)
        except:
            pass
    
    # Parse event start time
    event_start_str = market.get("eventStartTime")
    event_start_time = None
    if event_start_str:
        try:
            if event_start_str.endswith("Z"):
                event_start_str = event_start_str[:-1] + "+00:00"
            event_start_time = datetime.fromisoformat(event_start_str)
        except:
            pass
    
    # Determine asset from slug
    asset = "UNKNOWN"
    slug_lower = slug.lower()
    for a, short in ASSET_SHORT.items():
        if a in slug_lower:
            asset = short
            break
    
    # Get market-level prices from Gamma API (these are accurate!)
    best_bid = market.get("bestBid")
    best_ask = market.get("bestAsk")
    spread = market.get("spread")
    last_trade_price = market.get("lastTradePrice")
    
    # Convert to float if present
    best_bid = float(best_bid) if best_bid is not None else None
    best_ask = float(best_ask) if best_ask is not None else None
    spread = float(spread) if spread is not None else None
    last_trade_price = float(last_trade_price) if last_trade_price is not None else None
    
    return HourlyMarket(
        condition_id=condition_id,
        question=question,
        slug=slug,
        asset=asset,
        end_time=end_time,
        event_start_time=event_start_time,
        outcomes=market_outcomes,
        best_bid=best_bid,
        best_ask=best_ask,
        spread=spread,
        last_trade_price=last_trade_price,
        volume=float(market.get("volume", 0)),
        liquidity=float(market.get("liquidity", 0)),
    )


def display_market(market: HourlyMarket, now_utc: datetime):
    """Display market information with arbitrage analysis."""
    print(f"\n{'─' * 70}")
    print(f"📊 {market.question}")
    print(f"   Asset: {market.asset}")
    print(f"   Slug: {market.slug}")
    print(f"   Condition ID: {market.condition_id[:40]}...")
    
    # Time info
    if market.event_start_time:
        time_to_start = market.event_start_time - now_utc
        mins = time_to_start.total_seconds() / 60
        if mins > 0:
            print(f"   Event Start: {market.event_start_time.strftime('%Y-%m-%d %H:%M UTC')} (in {mins:.0f} min)")
        else:
            print(f"   Event Start: {market.event_start_time.strftime('%Y-%m-%d %H:%M UTC')} (STARTED {abs(mins):.0f} min ago)")
    
    if market.end_time:
        time_to_end = market.end_time - now_utc
        mins = time_to_end.total_seconds() / 60
        if mins > 0:
            print(f"   End Time: {market.end_time.strftime('%Y-%m-%d %H:%M UTC')} (in {mins:.0f} min)")
        else:
            print(f"   End Time: {market.end_time.strftime('%Y-%m-%d %H:%M UTC')} (ENDED {abs(mins):.0f} min ago)")
    
    print(f"   Volume: ${market.volume:,.2f}")
    print(f"   Liquidity: ${market.liquidity:,.2f}")
    
    # Market prices (from Gamma API - ACCURATE)
    print(f"\n   MARKET PRICES (Gamma API):")
    print(f"      Last Trade: ${market.last_trade_price:.4f}" if market.last_trade_price else "      Last Trade: N/A")
    print(f"      Best Bid: ${market.best_bid:.4f}" if market.best_bid else "      Best Bid: N/A")
    print(f"      Best Ask: ${market.best_ask:.4f}" if market.best_ask else "      Best Ask: N/A")
    print(f"      Spread: ${market.spread:.4f}" if market.spread else "      Spread: N/A")
    
    # Outcome prices
    print(f"\n   OUTCOME MID-PRICES:")
    for outcome in market.outcomes:
        print(f"      {outcome.outcome}: ${outcome.price:.4f}")
    
    # Verify prices sum to ~1.0
    total_price = sum(o.price for o in market.outcomes)
    print(f"      Sum: ${total_price:.4f}")
    
    # Arbitrage analysis
    # In a binary market, to buy BOTH outcomes:
    # - Buy Up at best_ask for Up
    # - Buy Down at (1 - best_bid for Up) = best_ask for Down
    # Since prices sum to ~1, if best_ask < outcome_price, there might be opportunity
    
    print(f"\n   💰 ARBITRAGE ANALYSIS:")
    
    if market.best_ask is not None and market.best_bid is not None:
        up_ask = market.best_ask  # Cost to buy Up
        down_ask = 1 - market.best_bid  # Cost to buy Down (complement of Up bid)
        
        total_cost = up_ask + down_ask
        
        print(f"      Cost to buy Up: ${up_ask:.4f}")
        print(f"      Cost to buy Down: ${down_ask:.4f} (= 1 - Up bid of ${market.best_bid:.4f})")
        print(f"      Total cost to buy both: ${total_cost:.4f}")
        
        if total_cost < 1.0:
            profit = 1.0 - total_cost
            profit_pct = (profit / total_cost) * 100
            print(f"      ✅ ARBITRAGE! Buy both for ${total_cost:.4f}, guaranteed $1.00 payout")
            print(f"         Profit: ${profit:.4f} per share ({profit_pct:.2f}%)")
        else:
            loss = total_cost - 1.0
            print(f"      ❌ No arbitrage (cost ${total_cost:.4f} >= $1.00, loss: ${loss:.4f})")
    else:
        print(f"      ⚠️ Market inactive (no bid/ask)")


async def main():
    print("=" * 80)
    print("HOURLY CRYPTO ARBITRAGE - DATA VERIFICATION")
    print("=" * 80)
    
    utc_now = datetime.now(timezone.utc)
    et_now = get_current_hour_et()
    
    print(f"\nCurrent time:")
    print(f"  UTC: {utc_now.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  ET:  {et_now.strftime('%Y-%m-%d %H:%M:%S')} (estimated)")
    
    async with httpx.AsyncClient(timeout=30.0) as http:
        found_markets = []
        
        print("\n" + "=" * 80)
        print("SEARCHING FOR HOURLY MARKETS:")
        print("=" * 80)
        
        # Check current hour and next few hours
        hours_to_check = list(range(-1, 4))  # From -1 to +3 hours relative to current ET hour
        
        for hour_offset in hours_to_check:
            check_time = et_now + timedelta(hours=hour_offset)
            check_time = check_time.replace(minute=0, second=0, microsecond=0)
            
            print(f"\n--- {check_time.strftime('%B %d, %I%p ET')} ---")
            
            for asset in ASSETS:
                slug = generate_event_slug(asset, check_time)
                short_name = ASSET_SHORT[asset]
                
                print(f"  {short_name}: {slug}...", end=" ")
                
                event = await fetch_event_by_slug(http, slug)
                
                if event:
                    print("✅ FOUND")
                    market = await get_market_data(http, event)
                    if market:
                        found_markets.append(market)
                else:
                    print("❌")
        
        # Display found markets
        if found_markets:
            print("\n" + "=" * 80)
            print("MARKET DATA:")
            print("=" * 80)
            
            # Sort by end time
            found_markets.sort(key=lambda m: m.end_time if m.end_time else datetime.max.replace(tzinfo=timezone.utc))
            
            for market in found_markets:
                display_market(market, utc_now)
            
            # Summary
            print("\n" + "=" * 80)
            print("SUMMARY:")
            print("=" * 80)
            
            active_markets = [m for m in found_markets if m.best_ask is not None]
            arb_opportunities = []
            
            for market in active_markets:
                up_ask = market.best_ask
                down_ask = 1 - market.best_bid if market.best_bid else None
                
                if up_ask and down_ask:
                    total_cost = up_ask + down_ask
                    if total_cost < 1.0:
                        profit = 1.0 - total_cost
                        arb_opportunities.append((market, total_cost, profit))
            
            print(f"\nTotal markets found: {len(found_markets)}")
            print(f"Active markets (with orderbook): {len(active_markets)}")
            print(f"  BTC: {len([m for m in active_markets if m.asset == 'BTC'])}")
            print(f"  ETH: {len([m for m in active_markets if m.asset == 'ETH'])}")
            print(f"  SOL: {len([m for m in active_markets if m.asset == 'SOL'])}")
            
            if arb_opportunities:
                print(f"\n🚨 ARBITRAGE OPPORTUNITIES: {len(arb_opportunities)}")
                for market, cost, profit in arb_opportunities:
                    print(f"  • {market.asset} {market.slug}")
                    print(f"    Cost: ${cost:.4f}, Profit: ${profit:.4f} ({profit*100:.2f}%)")
            else:
                print(f"\n❌ No arbitrage opportunities found (markets efficiently priced)")
            
            # Save data
            output = {
                "timestamp": utc_now.isoformat(),
                "markets": [
                    {
                        "slug": m.slug,
                        "asset": m.asset,
                        "question": m.question,
                        "condition_id": m.condition_id,
                        "end_time": m.end_time.isoformat() if m.end_time else None,
                        "event_start_time": m.event_start_time.isoformat() if m.event_start_time else None,
                        "volume": m.volume,
                        "liquidity": m.liquidity,
                        "best_bid": m.best_bid,
                        "best_ask": m.best_ask,
                        "spread": m.spread,
                        "last_trade_price": m.last_trade_price,
                        "outcomes": [
                            {
                                "name": o.outcome,
                                "token_id": o.token_id,
                                "price": o.price,
                            }
                            for o in m.outcomes
                        ]
                    }
                    for m in found_markets
                ]
            }
            
            with open("hourly_markets_data.json", "w") as f:
                json.dump(output, f, indent=2)
            
            print(f"\n📁 Saved data to hourly_markets_data.json")
            
        else:
            print("\n⚠️  No hourly markets found!")


if __name__ == "__main__":
    asyncio.run(main())
