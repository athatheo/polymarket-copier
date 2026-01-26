#!/usr/bin/env python3
"""
Backtesting script for the Crypto Edge Scanner strategy.

Fetches historical data and simulates the strategy to calculate performance metrics.
"""

import asyncio
import json
import math
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from typing import Optional
from pathlib import Path

import httpx

# =============================================================================
# CONFIGURATION (same as scanner)
# =============================================================================

# Entry thresholds (more selective)
ENTRY_EDGE_THRESHOLD = 0.08          # 8% edge required
MIN_MINUTES_TO_ENTER = 15            # Don't enter near expiry

# Profit taking (take profits earlier)
PROFIT_TARGET_PCT = 12               # 12% profit target
MIN_PROFIT_EDGE_EXIT = 5             # 5% profit if edge gone
EDGE_THRESHOLD_EXIT = 0.03           # 3% edge threshold

# Risk management (cut losses faster)
STOP_LOSS_PCT = -8                   # -8% stop loss
EDGE_REVERSAL_THRESHOLD = -0.05      # -5% edge reversal

# Time-based exits (avoid binary outcomes)
EXPIRY_LOCK_PROFIT_MINUTES = 10      # Lock profit within 10 min
MIN_PROFIT_NEAR_EXPIRY = 3           # 3% min profit to lock
FORCE_EXIT_MINUTES = 5               # Force exit within 5 min

# Position sizing
MAX_POSITION_USD = 50
MAX_TOTAL_EXPOSURE = 200

HOURLY_VOLATILITY = {
    "BTC": 0.5,
    "ETH": 0.7,
    "SOL": 1.0,
}

GAMMA_API_URL = "https://gamma-api.polymarket.com"
DATA_API_URL = "https://data-api.polymarket.com"


# =============================================================================
# DATA CLASSES
# =============================================================================

@dataclass
class HistoricalMarket:
    """Represents a historical hourly market."""
    market_id: str
    crypto: str
    title: str
    start_time: datetime
    end_time: datetime
    resolved_up: Optional[bool]  # True if resolved UP, False if DOWN, None if unresolved
    final_price_reference: float  # Crypto price at market open
    final_price_close: float     # Crypto price at market close


@dataclass
class PricePoint:
    """A single price point."""
    timestamp: datetime
    price: float


@dataclass
class SimulatedTrade:
    """Record of a simulated trade."""
    market_id: str
    crypto: str
    side: str  # "UP" or "DOWN"
    entry_time: datetime
    entry_price: float
    exit_time: Optional[datetime]
    exit_price: Optional[float]
    exit_reason: str
    size: float
    pnl_usd: float
    pnl_pct: float
    won: bool


@dataclass
class BacktestResult:
    """Results of a backtest run."""
    start_date: datetime
    end_date: datetime
    total_trades: int
    winning_trades: int
    losing_trades: int
    win_rate: float
    total_pnl_usd: float
    total_pnl_pct: float
    avg_pnl_per_trade: float
    max_drawdown: float
    sharpe_ratio: float
    trades: list[SimulatedTrade]


# =============================================================================
# STATISTICAL MODEL (same as scanner)
# =============================================================================

def normal_cdf(x: float) -> float:
    """Standard normal CDF approximation."""
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
    """Estimate fair probability that price will be UP at market close."""
    if minutes_remaining <= 0:
        return 1.0 if current_price > reference_price else 0.0
    
    if reference_price <= 0:
        return 0.5
    
    price_change_pct = (current_price - reference_price) / reference_price * 100
    remaining_vol = hourly_volatility * math.sqrt(minutes_remaining / 60)
    
    if remaining_vol <= 0.001:
        return 1.0 if price_change_pct > 0 else 0.0
    
    z_score = price_change_pct / remaining_vol
    fair_up_prob = normal_cdf(z_score)
    
    return max(0.01, min(0.99, fair_up_prob))


# =============================================================================
# HISTORICAL DATA FETCHING
# =============================================================================

async def fetch_historical_crypto_prices(
    crypto: str,
    start_date: datetime,
    end_date: datetime
) -> list[PricePoint]:
    """
    Fetch historical hourly prices from Binance API.
    
    Uses klines (candlestick) endpoint for hourly data.
    """
    symbols = {
        "BTC": "BTCUSDT",
        "ETH": "ETHUSDT",
        "SOL": "SOLUSDT",
    }
    
    symbol = symbols.get(crypto)
    if not symbol:
        return []
    
    # Binance klines endpoint
    url = "https://api.binance.com/api/v3/klines"
    
    # Convert to Unix timestamps (milliseconds)
    start_ts = int(start_date.timestamp() * 1000)
    end_ts = int(end_date.timestamp() * 1000)
    
    all_prices = []
    current_start = start_ts
    
    async with httpx.AsyncClient(timeout=30) as client:
        while current_start < end_ts:
            try:
                params = {
                    "symbol": symbol,
                    "interval": "1h",  # Hourly candles
                    "startTime": current_start,
                    "endTime": end_ts,
                    "limit": 1000,  # Max per request
                }
                
                response = await client.get(url, params=params)
                response.raise_for_status()
                data = response.json()
                
                if not data:
                    break
                
                for candle in data:
                    # Candle format: [open_time, open, high, low, close, volume, ...]
                    ts = datetime.fromtimestamp(candle[0] / 1000, tz=timezone.utc)
                    close_price = float(candle[4])  # Use close price
                    all_prices.append(PricePoint(timestamp=ts, price=close_price))
                
                # Move to next batch
                last_ts = data[-1][0]
                if last_ts <= current_start:
                    break
                current_start = last_ts + 1
                
                # Small delay to avoid rate limiting
                await asyncio.sleep(0.1)
                
            except Exception as e:
                print(f"Error fetching {crypto} prices: {e}")
                break
    
    return all_prices


async def fetch_historical_markets(
    crypto: str,
    days_back: int = 7
) -> list[dict]:
    """
    Fetch historical Up/Down markets from Polymarket.
    
    Note: This searches for closed markets. The Gamma API doesn't provide
    full historical data, so we use search with closed=true.
    """
    name_map = {
        "BTC": "Bitcoin",
        "ETH": "Ethereum",
        "SOL": "Solana"
    }
    
    full_name = name_map.get(crypto, crypto)
    search_term = f"{full_name} Up or Down"
    
    async with httpx.AsyncClient(timeout=30) as client:
        try:
            response = await client.get(
                f"{GAMMA_API_URL}/public-search",
                params={
                    "q": search_term,
                    "limit_per_type": 100,
                    "search_tags": "false",
                    "search_profiles": "false",
                    "keep_closed_markets": 1,
                }
            )
            response.raise_for_status()
            data = response.json()
            
            events = data.get("events", []) or []
            
            # Filter to closed Up/Down markets
            result = []
            for event in events:
                title = event.get("title", "").lower()
                if "up or down" in title and full_name.lower() in title:
                    if event.get("closed"):
                        result.append(event)
            
            return result
            
        except Exception as e:
            print(f"Error fetching {crypto} markets: {e}")
            return []


def parse_historical_market(event: dict, crypto: str) -> Optional[HistoricalMarket]:
    """Parse a historical market event."""
    markets = event.get("markets", [])
    if not markets:
        return None
    
    market = markets[0]
    
    # Parse times
    end_date_str = market.get("endDate") or event.get("endDate")
    if not end_date_str:
        return None
    
    try:
        end_time = datetime.fromisoformat(end_date_str.replace("Z", "+00:00"))
    except:
        return None
    
    # Estimate start time (1 hour before end for hourly markets)
    start_time = end_time - timedelta(hours=1)
    
    # Parse outcome prices to determine resolution
    prices_str = market.get("outcomePrices", "")
    try:
        prices = json.loads(prices_str) if prices_str else []
        prices = [float(p) for p in prices]
    except:
        prices = [0.5, 0.5]
    
    # If prices are 1 and 0, market is resolved
    resolved_up = None
    if prices and len(prices) >= 2:
        if prices[0] >= 0.99:
            resolved_up = True
        elif prices[0] <= 0.01:
            resolved_up = False
    
    return HistoricalMarket(
        market_id=market.get("id", ""),
        crypto=crypto,
        title=event.get("title", ""),
        start_time=start_time,
        end_time=end_time,
        resolved_up=resolved_up,
        final_price_reference=0,  # Will be filled from price data
        final_price_close=0,
    )


# =============================================================================
# PRICE INTERPOLATION
# =============================================================================

def interpolate_price(prices: list[PricePoint], target_time: datetime) -> float:
    """Get interpolated price at a specific time."""
    if not prices:
        return 0
    
    # Find bracketing prices
    before = None
    after = None
    
    for i, p in enumerate(prices):
        if p.timestamp <= target_time:
            before = p
        if p.timestamp >= target_time and after is None:
            after = p
            break
    
    if before is None and after is None:
        return prices[0].price if prices else 0
    
    if before is None:
        return after.price
    
    if after is None:
        return before.price
    
    if before.timestamp == after.timestamp:
        return before.price
    
    # Linear interpolation
    ratio = (target_time - before.timestamp).total_seconds() / \
            (after.timestamp - before.timestamp).total_seconds()
    
    return before.price + ratio * (after.price - before.price)


# =============================================================================
# SIMULATION ENGINE
# =============================================================================

@dataclass
class SimulationState:
    """Current state of the backtest simulation."""
    cash: float
    positions: list[dict] = field(default_factory=list)
    trades: list[SimulatedTrade] = field(default_factory=list)
    equity_curve: list[tuple[datetime, float]] = field(default_factory=list)


def simulate_market(
    market: HistoricalMarket,
    prices: list[PricePoint],
    state: SimulationState,
    check_interval_minutes: int = 5
) -> None:
    """
    Simulate trading a single market.
    
    This simulates checking for opportunities at regular intervals,
    entering when edge is detected, and managing the position.
    """
    if market.resolved_up is None:
        return  # Skip unresolved markets
    
    crypto = market.crypto
    hourly_vol = HOURLY_VOLATILITY.get(crypto, 0.5)
    
    # Get reference price (at market start)
    reference_price = interpolate_price(prices, market.start_time)
    if reference_price <= 0:
        return
    
    market.final_price_reference = reference_price
    market.final_price_close = interpolate_price(prices, market.end_time)
    
    # Simulate checking at intervals
    current_time = market.start_time + timedelta(minutes=check_interval_minutes)
    position = None
    
    while current_time < market.end_time:
        minutes_remaining = (market.end_time - current_time).total_seconds() / 60
        
        if minutes_remaining < MIN_MINUTES_TO_ENTER and position is None:
            break  # Too close to expiry to enter
        
        current_price = interpolate_price(prices, current_time)
        if current_price <= 0:
            current_time += timedelta(minutes=check_interval_minutes)
            continue
        
        # Calculate fair probability
        fair_prob = estimate_fair_probability(
            reference_price, current_price, minutes_remaining, hourly_vol
        )
        
        # Simulate market price with realistic dynamics
        # Markets start at 50/50, then converge towards fair value over time
        # Convergence accelerates as expiry approaches (more information)
        elapsed_ratio = (current_time - market.start_time).total_seconds() / 3600
        
        # Market efficiency increases over time (starts slow, gets faster near expiry)
        # At 0% elapsed: 20% efficient, at 50% elapsed: 50% efficient, at 90%: 80% efficient
        market_efficiency = 0.2 + 0.7 * (elapsed_ratio ** 0.5)
        
        # Add some noise/randomness to simulate market inefficiency
        import random
        noise = random.gauss(0, 0.03)  # 3% standard deviation noise
        
        simulated_market_prob = 0.5 + (fair_prob - 0.5) * market_efficiency + noise
        simulated_market_prob = max(0.05, min(0.95, simulated_market_prob))
        
        # === POSITION MANAGEMENT ===
        if position:
            # Get current token price
            if position["side"] == "UP":
                current_token_price = simulated_market_prob
            else:
                current_token_price = 1 - simulated_market_prob
            
            profit_pct = (current_token_price - position["entry_price"]) / position["entry_price"] * 100
            
            # Calculate edge from our perspective
            if position["side"] == "UP":
                our_fair_value = fair_prob
            else:
                our_fair_value = 1 - fair_prob
            current_edge = our_fair_value - current_token_price
            
            # Check exit conditions (ordered by priority)
            should_exit = False
            exit_reason = ""
            
            # 0. FORCE EXIT - avoid binary outcome at all costs
            if minutes_remaining <= FORCE_EXIT_MINUTES:
                should_exit = True
                exit_reason = f"FORCE_EXIT: {profit_pct:+.1f}%, {minutes_remaining:.0f}min left"
            # 1. PROFIT TARGET
            elif profit_pct >= PROFIT_TARGET_PCT:
                should_exit = True
                exit_reason = f"PROFIT_TARGET: +{profit_pct:.1f}%"
            # 2. STOP LOSS - cut losses quickly
            elif profit_pct <= STOP_LOSS_PCT:
                should_exit = True
                exit_reason = f"STOP_LOSS: {profit_pct:.1f}%"
            # 3. EDGE EVAPORATED with profit
            elif profit_pct >= MIN_PROFIT_EDGE_EXIT and current_edge < EDGE_THRESHOLD_EXIT:
                should_exit = True
                exit_reason = f"EDGE_EVAPORATED: +{profit_pct:.1f}%"
            # 4. NEAR EXPIRY with profit
            elif minutes_remaining <= EXPIRY_LOCK_PROFIT_MINUTES and profit_pct >= MIN_PROFIT_NEAR_EXPIRY:
                should_exit = True
                exit_reason = f"NEAR_EXPIRY: +{profit_pct:.1f}%"
            # 5. EDGE REVERSED (exit even with small loss)
            elif current_edge <= EDGE_REVERSAL_THRESHOLD and profit_pct < 2:
                should_exit = True
                exit_reason = f"EDGE_REVERSED: {profit_pct:+.1f}%"
            
            if should_exit:
                # Close position
                pnl_usd = (current_token_price - position["entry_price"]) * position["size"]
                
                trade = SimulatedTrade(
                    market_id=market.market_id,
                    crypto=crypto,
                    side=position["side"],
                    entry_time=position["entry_time"],
                    entry_price=position["entry_price"],
                    exit_time=current_time,
                    exit_price=current_token_price,
                    exit_reason=exit_reason,
                    size=position["size"],
                    pnl_usd=pnl_usd,
                    pnl_pct=profit_pct,
                    won=pnl_usd > 0,
                )
                state.trades.append(trade)
                state.cash += MAX_POSITION_USD + pnl_usd
                position = None
        
        # === ENTRY LOGIC ===
        if position is None and state.cash >= MAX_POSITION_USD:
            # Calculate edge
            edge_up = fair_prob - simulated_market_prob
            edge_magnitude = abs(edge_up)
            
            if edge_magnitude >= ENTRY_EDGE_THRESHOLD:
                side = "UP" if edge_up > 0 else "DOWN"
                entry_price = simulated_market_prob if side == "UP" else (1 - simulated_market_prob)
                size = MAX_POSITION_USD / entry_price
                
                position = {
                    "side": side,
                    "entry_price": entry_price,
                    "entry_time": current_time,
                    "size": size,
                    "reference_price": reference_price,
                }
                state.cash -= MAX_POSITION_USD
        
        # Record equity
        equity = state.cash
        if position:
            if position["side"] == "UP":
                current_token_price = simulated_market_prob
            else:
                current_token_price = 1 - simulated_market_prob
            equity += current_token_price * position["size"]
        state.equity_curve.append((current_time, equity))
        
        current_time += timedelta(minutes=check_interval_minutes)
    
    # === MARKET EXPIRY ===
    if position:
        # Market resolved - calculate final P&L
        if position["side"] == "UP":
            final_price = 1.0 if market.resolved_up else 0.0
        else:
            final_price = 0.0 if market.resolved_up else 1.0
        
        pnl_usd = (final_price - position["entry_price"]) * position["size"]
        pnl_pct = (final_price - position["entry_price"]) / position["entry_price"] * 100
        
        trade = SimulatedTrade(
            market_id=market.market_id,
            crypto=crypto,
            side=position["side"],
            entry_time=position["entry_time"],
            entry_price=position["entry_price"],
            exit_time=market.end_time,
            exit_price=final_price,
            exit_reason="MARKET_EXPIRY",
            size=position["size"],
            pnl_usd=pnl_usd,
            pnl_pct=pnl_pct,
            won=pnl_usd > 0,
        )
        state.trades.append(trade)
        state.cash += MAX_POSITION_USD + pnl_usd


def calculate_max_drawdown(equity_curve: list[tuple[datetime, float]]) -> float:
    """Calculate maximum drawdown from equity curve."""
    if not equity_curve:
        return 0.0
    
    equities = [e[1] for e in equity_curve]
    peak = equities[0]
    max_dd = 0
    
    for equity in equities:
        if equity > peak:
            peak = equity
        drawdown = (peak - equity) / peak if peak > 0 else 0
        max_dd = max(max_dd, drawdown)
    
    return max_dd


def calculate_sharpe_ratio(trades: list[SimulatedTrade]) -> float:
    """Calculate Sharpe ratio from trades."""
    if len(trades) < 2:
        return 0.0
    
    returns = [t.pnl_pct / 100 for t in trades]
    avg_return = sum(returns) / len(returns)
    
    variance = sum((r - avg_return) ** 2 for r in returns) / len(returns)
    std_dev = math.sqrt(variance)
    
    if std_dev == 0:
        return 0.0
    
    # Annualized (assuming ~8760 hours per year, ~1 trade per hour opportunity)
    return (avg_return / std_dev) * math.sqrt(8760)


# =============================================================================
# MAIN BACKTEST
# =============================================================================

async def run_backtest(
    days_back: int = 7,
    initial_capital: float = 1000.0
) -> BacktestResult:
    """
    Run backtest over historical data.
    """
    print("=" * 80)
    print("  CRYPTO EDGE SCANNER - BACKTEST")
    print("=" * 80)
    
    end_date = datetime.now(timezone.utc)
    start_date = end_date - timedelta(days=days_back)
    
    print(f"\nPeriod: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
    print(f"Initial Capital: ${initial_capital:.2f}")
    print(f"Position Size: ${MAX_POSITION_USD:.2f}")
    print(f"Entry Threshold: {ENTRY_EDGE_THRESHOLD:.1%}")
    
    # Initialize state
    state = SimulationState(cash=initial_capital)
    
    # Fetch historical prices for all cryptos
    all_prices = {}
    for crypto in ["BTC", "ETH", "SOL"]:
        print(f"\nFetching {crypto} price history...")
        prices = await fetch_historical_crypto_prices(crypto, start_date, end_date)
        all_prices[crypto] = prices
        print(f"  Got {len(prices)} price points")
    
    # Fetch historical markets
    all_markets = []
    for crypto in ["BTC", "ETH", "SOL"]:
        print(f"\nFetching {crypto} historical markets...")
        events = await fetch_historical_markets(crypto, days_back)
        
        for event in events:
            market = parse_historical_market(event, crypto)
            if market and market.start_time >= start_date:
                all_markets.append(market)
        
        print(f"  Found {len([m for m in all_markets if m.crypto == crypto])} markets")
    
    # Sort markets by start time
    all_markets.sort(key=lambda m: m.start_time)
    
    print(f"\nTotal markets to simulate: {len(all_markets)}")
    print("\nRunning simulation...")
    
    # Simulate each market
    for i, market in enumerate(all_markets):
        if (i + 1) % 50 == 0:
            print(f"  Processed {i + 1}/{len(all_markets)} markets...")
        
        prices = all_prices.get(market.crypto, [])
        if prices:
            simulate_market(market, prices, state)
    
    # Calculate results
    trades = state.trades
    winning_trades = [t for t in trades if t.won]
    losing_trades = [t for t in trades if not t.won]
    
    total_pnl = sum(t.pnl_usd for t in trades)
    win_rate = len(winning_trades) / len(trades) if trades else 0
    
    result = BacktestResult(
        start_date=start_date,
        end_date=end_date,
        total_trades=len(trades),
        winning_trades=len(winning_trades),
        losing_trades=len(losing_trades),
        win_rate=win_rate,
        total_pnl_usd=total_pnl,
        total_pnl_pct=(total_pnl / initial_capital) * 100,
        avg_pnl_per_trade=total_pnl / len(trades) if trades else 0,
        max_drawdown=calculate_max_drawdown(state.equity_curve),
        sharpe_ratio=calculate_sharpe_ratio(trades),
        trades=trades,
    )
    
    return result


def print_backtest_results(result: BacktestResult):
    """Print formatted backtest results."""
    print("\n" + "=" * 80)
    print("  BACKTEST RESULTS")
    print("=" * 80)
    
    print(f"\n📅 PERIOD")
    print(f"  Start: {result.start_date.strftime('%Y-%m-%d')}")
    print(f"  End:   {result.end_date.strftime('%Y-%m-%d')}")
    
    print(f"\n📊 TRADE STATISTICS")
    print("-" * 40)
    print(f"  Total Trades:    {result.total_trades}")
    print(f"  Winning Trades:  {result.winning_trades}")
    print(f"  Losing Trades:   {result.losing_trades}")
    print(f"  Win Rate:        {result.win_rate:.1%}")
    
    print(f"\n💰 PERFORMANCE")
    print("-" * 40)
    print(f"  Total P&L:       ${result.total_pnl_usd:+.2f} ({result.total_pnl_pct:+.1f}%)")
    print(f"  Avg P&L/Trade:   ${result.avg_pnl_per_trade:+.2f}")
    print(f"  Max Drawdown:    {result.max_drawdown:.1%}")
    print(f"  Sharpe Ratio:    {result.sharpe_ratio:.2f}")
    
    # Trade breakdown by crypto
    print(f"\n📈 BY CRYPTOCURRENCY")
    print("-" * 40)
    for crypto in ["BTC", "ETH", "SOL"]:
        crypto_trades = [t for t in result.trades if t.crypto == crypto]
        if crypto_trades:
            wins = len([t for t in crypto_trades if t.won])
            pnl = sum(t.pnl_usd for t in crypto_trades)
            print(f"  {crypto}:")
            print(f"    Trades: {len(crypto_trades)}, Wins: {wins}, P&L: ${pnl:+.2f}")
    
    # Trade breakdown by side
    print(f"\n📊 BY SIDE")
    print("-" * 40)
    for side in ["UP", "DOWN"]:
        side_trades = [t for t in result.trades if t.side == side]
        if side_trades:
            wins = len([t for t in side_trades if t.won])
            pnl = sum(t.pnl_usd for t in side_trades)
            print(f"  {side}:")
            print(f"    Trades: {len(side_trades)}, Wins: {wins}, P&L: ${pnl:+.2f}")
    
    # Exit reason breakdown
    print(f"\n🚪 EXIT REASONS")
    print("-" * 40)
    exit_reasons = {}
    for trade in result.trades:
        reason = trade.exit_reason.split(":")[0]
        if reason not in exit_reasons:
            exit_reasons[reason] = {"count": 0, "pnl": 0}
        exit_reasons[reason]["count"] += 1
        exit_reasons[reason]["pnl"] += trade.pnl_usd
    
    for reason, data in sorted(exit_reasons.items(), key=lambda x: -x[1]["count"]):
        print(f"  {reason}: {data['count']} trades, ${data['pnl']:+.2f}")
    
    # Sample trades
    print(f"\n📝 SAMPLE TRADES (last 10)")
    print("-" * 80)
    print(f"{'Crypto':<6} {'Side':<6} {'Entry':>8} {'Exit':>8} {'P&L':>10} {'Reason':<20}")
    print("-" * 80)
    for trade in result.trades[-10:]:
        print(f"{trade.crypto:<6} {trade.side:<6} "
              f"${trade.entry_price:>6.4f} ${trade.exit_price or 0:>6.4f} "
              f"${trade.pnl_usd:>+8.2f} {trade.exit_reason[:20]:<20}")
    
    print("\n" + "=" * 80)


async def main():
    """Entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Backtest Crypto Edge Strategy")
    parser.add_argument("--days", type=int, default=7, help="Days to backtest")
    parser.add_argument("--capital", type=float, default=1000, help="Initial capital")
    parser.add_argument("--save", type=str, help="Save results to JSON file")
    args = parser.parse_args()
    
    result = await run_backtest(
        days_back=args.days,
        initial_capital=args.capital
    )
    
    print_backtest_results(result)
    
    if args.save:
        # Save results to JSON
        output = {
            "start_date": result.start_date.isoformat(),
            "end_date": result.end_date.isoformat(),
            "total_trades": result.total_trades,
            "winning_trades": result.winning_trades,
            "losing_trades": result.losing_trades,
            "win_rate": result.win_rate,
            "total_pnl_usd": result.total_pnl_usd,
            "total_pnl_pct": result.total_pnl_pct,
            "avg_pnl_per_trade": result.avg_pnl_per_trade,
            "max_drawdown": result.max_drawdown,
            "sharpe_ratio": result.sharpe_ratio,
            "trades": [
                {
                    "market_id": t.market_id,
                    "crypto": t.crypto,
                    "side": t.side,
                    "entry_time": t.entry_time.isoformat(),
                    "entry_price": t.entry_price,
                    "exit_time": t.exit_time.isoformat() if t.exit_time else None,
                    "exit_price": t.exit_price,
                    "exit_reason": t.exit_reason,
                    "pnl_usd": t.pnl_usd,
                    "pnl_pct": t.pnl_pct,
                    "won": t.won,
                }
                for t in result.trades
            ]
        }
        
        with open(args.save, "w") as f:
            json.dump(output, f, indent=2)
        print(f"\nResults saved to {args.save}")


if __name__ == "__main__":
    asyncio.run(main())
