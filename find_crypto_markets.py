"""
Script to find crypto price prediction markets on Polymarket.
Searches for Bitcoin, Ethereum, Solana, and XRP hourly markets.
"""

import httpx
import json
from datetime import datetime

GAMMA_API_URL = "https://gamma-api.polymarket.com"


def search_markets(query: str, limit: int = 20):
    """Search for markets matching a query."""
    url = f"{GAMMA_API_URL}/public-search"
    params = {
        "q": query,
        "limit_per_type": limit,
        "search_tags": "false",
        "search_profiles": "false",
    }
    
    response = httpx.get(url, params=params, timeout=30)
    response.raise_for_status()
    return response.json()


def format_market(market: dict) -> dict:
    """Extract key market information."""
    return {
        "id": market.get("id"),
        "condition_id": market.get("conditionId"),
        "question": market.get("question"),
        "slug": market.get("slug"),
        "outcomes": market.get("outcomes"),
        "outcome_prices": market.get("outcomePrices"),
        "volume": market.get("volume"),
        "liquidity": market.get("liquidity"),
        "end_date": market.get("endDate"),
        "active": market.get("active"),
        "closed": market.get("closed"),
        "clob_token_ids": market.get("clobTokenIds"),
    }


def format_event(event: dict) -> dict:
    """Extract key event information."""
    return {
        "id": event.get("id"),
        "title": event.get("title"),
        "slug": event.get("slug"),
        "description": event.get("description", "")[:200],
        "active": event.get("active"),
        "closed": event.get("closed"),
        "volume": event.get("volume"),
        "liquidity": event.get("liquidity"),
        "end_date": event.get("endDate"),
        "markets": [format_market(m) for m in event.get("markets", [])]
    }


def main():
    # Search terms for crypto hourly predictions
    search_terms = [
        "bitcoin hourly",
        "btc hour",
        "ethereum hourly",
        "eth hour",
        "solana hourly", 
        "sol hour",
        "xrp hourly",
        "ripple hour",
        "crypto hourly",
        "bitcoin up",
        "bitcoin down",
    ]
    
    all_events = {}
    all_series = {}
    
    for term in search_terms:
        print(f"\n{'='*60}")
        print(f"Searching for: {term}")
        print('='*60)
        
        try:
            results = search_markets(term)
            
            # Process events
            events = results.get("events", []) or []
            for event in events:
                event_id = event.get("id")
                if event_id and event_id not in all_events:
                    title = event.get("title", "")
                    # Filter for hourly/price related events
                    if any(kw in title.lower() for kw in ["hour", "price", "up", "down", "bitcoin", "btc", "ethereum", "eth", "solana", "sol", "xrp", "ripple"]):
                        all_events[event_id] = format_event(event)
                        print(f"\nEvent: {title}")
                        print(f"  Active: {event.get('active')}, Closed: {event.get('closed')}")
                        print(f"  Volume: ${event.get('volume', 0):,.0f}")
                        print(f"  End Date: {event.get('endDate')}")
                        
                        markets = event.get("markets", [])
                        for market in markets[:3]:  # Show first 3 markets
                            print(f"    Market: {market.get('question', 'N/A')}")
                            print(f"      Token IDs: {market.get('clobTokenIds')}")
                            print(f"      Prices: {market.get('outcomePrices')}")
            
            # Process series (recurring markets)
            series = results.get("series", []) or []
            for s in series:
                series_id = s.get("id")
                if series_id and series_id not in all_series:
                    title = s.get("title", "")
                    if any(kw in title.lower() for kw in ["hour", "bitcoin", "btc", "ethereum", "eth", "solana", "sol", "xrp", "crypto"]):
                        all_series[series_id] = s
                        print(f"\nSeries: {title}")
                        print(f"  Slug: {s.get('slug')}")
                        print(f"  Recurrence: {s.get('recurrence')}")
                        print(f"  Active: {s.get('active')}")
                        
        except Exception as e:
            print(f"Error searching for '{term}': {e}")
    
    # Summary
    print(f"\n{'='*60}")
    print("SUMMARY")
    print('='*60)
    print(f"Found {len(all_events)} unique events")
    print(f"Found {len(all_series)} unique series")
    
    # Save results to JSON for further analysis
    output = {
        "search_time": datetime.now().isoformat(),
        "events": list(all_events.values()),
        "series": list(all_series.values()),
    }
    
    with open("crypto_markets_search_results.json", "w") as f:
        json.dump(output, f, indent=2, default=str)
    
    print(f"\nResults saved to crypto_markets_search_results.json")


if __name__ == "__main__":
    main()
