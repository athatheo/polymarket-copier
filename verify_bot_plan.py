"""
Verification script: Test that hourly_trading_bot.py matches the plan specifications.

Tests:
1. State Machine states and transitions
2. Decision rules (entry, profit-take, hedge, re-entry)
3. Position Manager functionality
4. Configuration parameters
5. Risk controls
"""

import sys
from datetime import datetime, timezone, timedelta

# Import the bot components
from hourly_trading_bot import (
    PositionState, Action,
    Position, MarketPosition,
    PositionManager, TradingStateMachine, TradeLogger,
    POSITION_SIZE_USD, MAX_EXPOSURE_PER_MARKET, MAX_TOTAL_EXPOSURE,
    PROFIT_TAKE_PCT, HEDGE_TRIGGER_PCT, RE_ENTRY_COOLDOWN_SEC,
    POLL_INTERVAL_SEC, MAX_SPREAD_TO_ENTER, MIN_MINUTES_TO_TRADE,
    ASSETS,
)


class Colors:
    GREEN = '\033[92m'
    RED = '\033[91m'
    YELLOW = '\033[93m'
    RESET = '\033[0m'
    BOLD = '\033[1m'


def pass_test(name: str):
    print(f"  {Colors.GREEN}✓{Colors.RESET} {name}")


def fail_test(name: str, expected, actual):
    print(f"  {Colors.RED}✗{Colors.RESET} {name}")
    print(f"    Expected: {expected}")
    print(f"    Actual: {actual}")
    return False


def section(name: str):
    print(f"\n{Colors.BOLD}=== {name} ==={Colors.RESET}")


def verify_configuration_parameters():
    """Verify config parameters match the plan."""
    section("1. Configuration Parameters (Plan Section 4)")
    all_passed = True
    
    # From plan:
    # POSITION_SIZE_USD = 100
    # MAX_EXPOSURE_PER_MARKET = 200
    # MAX_TOTAL_EXPOSURE = 500
    # PROFIT_TAKE_PCT = 15
    # HEDGE_TRIGGER_PCT = -10
    # RE_ENTRY_COOLDOWN_SEC = 30
    # POLL_INTERVAL_SEC = 15
    # MAX_SPREAD_TO_ENTER = 0.03
    # MIN_MINUTES_TO_TRADE = 5
    
    tests = [
        ("POSITION_SIZE_USD", POSITION_SIZE_USD, 100),
        ("MAX_EXPOSURE_PER_MARKET", MAX_EXPOSURE_PER_MARKET, 200),
        ("MAX_TOTAL_EXPOSURE", MAX_TOTAL_EXPOSURE, 500),
        ("PROFIT_TAKE_PCT", PROFIT_TAKE_PCT, 15),
        ("HEDGE_TRIGGER_PCT", HEDGE_TRIGGER_PCT, -10),
        ("RE_ENTRY_COOLDOWN_SEC", RE_ENTRY_COOLDOWN_SEC, 30),
        ("POLL_INTERVAL_SEC", POLL_INTERVAL_SEC, 15),
        ("MAX_SPREAD_TO_ENTER", MAX_SPREAD_TO_ENTER, 0.03),
        ("MIN_MINUTES_TO_TRADE", MIN_MINUTES_TO_TRADE, 5),
    ]
    
    for name, actual, expected in tests:
        if actual == expected:
            pass_test(f"{name} = {expected}")
        else:
            all_passed = fail_test(f"{name}", expected, actual)
    
    return all_passed


def verify_state_machine_states():
    """Verify state machine has all required states."""
    section("2. State Machine States (Plan: NONE, LONG_DOWN, LONG_UP, HEDGED)")
    all_passed = True
    
    required_states = ["NONE", "LONG_DOWN", "LONG_UP", "HEDGED"]
    actual_states = [s.value for s in PositionState]
    
    for state in required_states:
        if state in actual_states:
            pass_test(f"State {state} exists")
        else:
            all_passed = fail_test(f"State {state}", "exists", "missing")
    
    return all_passed


def verify_actions():
    """Verify all required actions exist."""
    section("3. Actions (Plan: HOLD, BUY_DOWN, BUY_UP, SELL_DOWN, SELL_UP)")
    all_passed = True
    
    required_actions = ["HOLD", "BUY_DOWN", "BUY_UP", "SELL_DOWN", "SELL_UP"]
    actual_actions = [a.value for a in Action]
    
    for action in required_actions:
        if action in actual_actions:
            pass_test(f"Action {action} exists")
        else:
            all_passed = fail_test(f"Action {action}", "exists", "missing")
    
    return all_passed


def verify_entry_rules():
    """Verify entry decision rules match plan."""
    section("4. Entry Rules")
    all_passed = True
    sm = TradingStateMachine()
    
    # Rule: "Initial bias: buy Down at hour start"
    action = sm.decide(
        state=PositionState.NONE,
        binance_pct=0.0,
        position_pnl_pct=0.0,
        minutes_remaining=55,
        spread=0.01,
        can_trade_cooldown=True,
        is_initial_entry=True,
    )
    if action == Action.BUY_DOWN:
        pass_test("Initial entry: BUY_DOWN at hour start")
    else:
        all_passed = fail_test("Initial entry", Action.BUY_DOWN, action)
    
    # Rule: "Re-entry after profit: follow current Binance trend (down)"
    action = sm.decide(
        state=PositionState.NONE,
        binance_pct=-0.5,  # Price trending down
        position_pnl_pct=0.0,
        minutes_remaining=40,
        spread=0.01,
        can_trade_cooldown=True,
        is_initial_entry=False,
    )
    if action == Action.BUY_DOWN:
        pass_test("Re-entry when BTC trending DOWN: BUY_DOWN")
    else:
        all_passed = fail_test("Re-entry trending down", Action.BUY_DOWN, action)
    
    # Rule: "Re-entry after profit: follow current Binance trend (up)"
    action = sm.decide(
        state=PositionState.NONE,
        binance_pct=0.5,  # Price trending up
        position_pnl_pct=0.0,
        minutes_remaining=40,
        spread=0.01,
        can_trade_cooldown=True,
        is_initial_entry=False,
    )
    if action == Action.BUY_UP:
        pass_test("Re-entry when BTC trending UP: BUY_UP")
    else:
        all_passed = fail_test("Re-entry trending up", Action.BUY_UP, action)
    
    # Rule: "Entry price must have reasonable spread (<3%)"
    action = sm.decide(
        state=PositionState.NONE,
        binance_pct=0.0,
        position_pnl_pct=0.0,
        minutes_remaining=40,
        spread=0.05,  # 5% spread - too wide
        can_trade_cooldown=True,
        is_initial_entry=True,
    )
    if action == Action.HOLD:
        pass_test("Wide spread (5%) blocks entry: HOLD")
    else:
        all_passed = fail_test("Wide spread blocks entry", Action.HOLD, action)
    
    # Rule: "Stop new entries in last 5 minutes"
    action = sm.decide(
        state=PositionState.NONE,
        binance_pct=0.0,
        position_pnl_pct=0.0,
        minutes_remaining=3,  # Only 3 minutes left
        spread=0.01,
        can_trade_cooldown=True,
        is_initial_entry=True,
    )
    if action == Action.HOLD:
        pass_test("No new entries in last 5 minutes: HOLD")
    else:
        all_passed = fail_test("Last 5 minutes blocks entry", Action.HOLD, action)
    
    # Rule: Cooldown blocks re-entry
    action = sm.decide(
        state=PositionState.NONE,
        binance_pct=-0.5,
        position_pnl_pct=0.0,
        minutes_remaining=40,
        spread=0.01,
        can_trade_cooldown=False,  # Still in cooldown
        is_initial_entry=False,
    )
    if action == Action.HOLD:
        pass_test("Cooldown blocks re-entry: HOLD")
    else:
        all_passed = fail_test("Cooldown blocks re-entry", Action.HOLD, action)
    
    return all_passed


def verify_profit_take_rules():
    """Verify profit-take rules match plan."""
    section("5. Profit-Take Rules (Plan: Exit at +15% gain)")
    all_passed = True
    sm = TradingStateMachine()
    
    # Rule: "Position up 15%+ → sell entire position" (LONG_DOWN)
    action = sm.decide(
        state=PositionState.LONG_DOWN,
        binance_pct=-1.0,
        position_pnl_pct=16.0,  # +16% profit
        minutes_remaining=40,
        spread=0.01,
        can_trade_cooldown=True,
    )
    if action == Action.SELL_DOWN:
        pass_test("LONG_DOWN +16% profit: SELL_DOWN")
    else:
        all_passed = fail_test("Profit take DOWN", Action.SELL_DOWN, action)
    
    # Rule: "Position up 15%+ → sell entire position" (LONG_UP)
    action = sm.decide(
        state=PositionState.LONG_UP,
        binance_pct=1.0,
        position_pnl_pct=15.0,  # Exactly +15%
        minutes_remaining=40,
        spread=0.01,
        can_trade_cooldown=True,
    )
    if action == Action.SELL_UP:
        pass_test("LONG_UP +15% profit: SELL_UP")
    else:
        all_passed = fail_test("Profit take UP", Action.SELL_UP, action)
    
    # Below threshold - should HOLD
    action = sm.decide(
        state=PositionState.LONG_DOWN,
        binance_pct=-0.5,
        position_pnl_pct=10.0,  # +10% - below threshold
        minutes_remaining=40,
        spread=0.01,
        can_trade_cooldown=True,
    )
    if action == Action.HOLD:
        pass_test("LONG_DOWN +10% (below threshold): HOLD")
    else:
        all_passed = fail_test("Below profit threshold", Action.HOLD, action)
    
    return all_passed


def verify_hedge_rules():
    """Verify hedge rules match plan."""
    section("6. Hedge Rules (Plan: Add hedge at -10%)")
    all_passed = True
    sm = TradingStateMachine()
    
    # Rule: "Position down 10%+ → buy opposite side" (LONG_DOWN losing)
    action = sm.decide(
        state=PositionState.LONG_DOWN,
        binance_pct=1.0,  # Price going up (bad for DOWN)
        position_pnl_pct=-12.0,  # -12% loss
        minutes_remaining=40,
        spread=0.01,
        can_trade_cooldown=True,
    )
    if action == Action.BUY_UP:
        pass_test("LONG_DOWN -12% loss: hedge with BUY_UP")
    else:
        all_passed = fail_test("Hedge DOWN position", Action.BUY_UP, action)
    
    # Rule: "Position down 10%+ → buy opposite side" (LONG_UP losing)
    action = sm.decide(
        state=PositionState.LONG_UP,
        binance_pct=-1.0,  # Price going down (bad for UP)
        position_pnl_pct=-10.0,  # Exactly -10%
        minutes_remaining=40,
        spread=0.01,
        can_trade_cooldown=True,
    )
    if action == Action.BUY_DOWN:
        pass_test("LONG_UP -10% loss: hedge with BUY_DOWN")
    else:
        all_passed = fail_test("Hedge UP position", Action.BUY_DOWN, action)
    
    # Above threshold (less negative) - should HOLD
    action = sm.decide(
        state=PositionState.LONG_DOWN,
        binance_pct=0.5,
        position_pnl_pct=-5.0,  # -5% - not bad enough to hedge
        minutes_remaining=40,
        spread=0.01,
        can_trade_cooldown=True,
    )
    if action == Action.HOLD:
        pass_test("LONG_DOWN -5% (above threshold): HOLD")
    else:
        all_passed = fail_test("Above hedge threshold", Action.HOLD, action)
    
    return all_passed


def verify_hedged_state():
    """Verify hedged state behavior."""
    section("7. Hedged State Rules")
    all_passed = True
    sm = TradingStateMachine()
    
    # Rule: "Near hour end - pick the winning side" (Down winning)
    action = sm.decide(
        state=PositionState.HEDGED,
        binance_pct=-0.5,  # Price down, DOWN likely to win
        position_pnl_pct=0.0,
        minutes_remaining=8,  # Near hour end
        spread=0.01,
        can_trade_cooldown=True,
    )
    if action == Action.SELL_UP:
        pass_test("HEDGED near end, DOWN winning: SELL_UP (keep DOWN)")
    else:
        all_passed = fail_test("Hedged exit DOWN winning", Action.SELL_UP, action)
    
    # Rule: "Near hour end - pick the winning side" (Up winning)
    action = sm.decide(
        state=PositionState.HEDGED,
        binance_pct=0.5,  # Price up, UP likely to win
        position_pnl_pct=0.0,
        minutes_remaining=8,
        spread=0.01,
        can_trade_cooldown=True,
    )
    if action == Action.SELL_DOWN:
        pass_test("HEDGED near end, UP winning: SELL_DOWN (keep UP)")
    else:
        all_passed = fail_test("Hedged exit UP winning", Action.SELL_DOWN, action)
    
    # Mid-hour hedged - should hold
    action = sm.decide(
        state=PositionState.HEDGED,
        binance_pct=0.1,
        position_pnl_pct=0.0,
        minutes_remaining=30,
        spread=0.01,
        can_trade_cooldown=True,
    )
    if action == Action.HOLD:
        pass_test("HEDGED mid-hour: HOLD")
    else:
        all_passed = fail_test("Hedged mid-hour", Action.HOLD, action)
    
    return all_passed


def verify_position_manager():
    """Verify position manager functionality."""
    section("8. Position Manager")
    all_passed = True
    pm = PositionManager()
    
    # Test initial state
    state = pm.get_state("BTC")
    if state == PositionState.NONE:
        pass_test("Initial state: NONE")
    else:
        all_passed = fail_test("Initial state", PositionState.NONE, state)
    
    # Test opening DOWN position
    pm.open_position("BTC", "DOWN", 0.50, 100.0)
    state = pm.get_state("BTC")
    if state == PositionState.LONG_DOWN:
        pass_test("After BUY DOWN: LONG_DOWN")
    else:
        all_passed = fail_test("After BUY DOWN", PositionState.LONG_DOWN, state)
    
    # Test exposure tracking
    exposure = pm.get_total_exposure()
    if exposure == 100.0:
        pass_test("Exposure after $100 position: $100")
    else:
        all_passed = fail_test("Exposure tracking", 100.0, exposure)
    
    # Test adding hedge (opens UP position)
    pm.open_position("BTC", "UP", 0.50, 100.0)
    state = pm.get_state("BTC")
    if state == PositionState.HEDGED:
        pass_test("After adding UP: HEDGED")
    else:
        all_passed = fail_test("After adding UP", PositionState.HEDGED, state)
    
    # Test exposure after hedge
    exposure = pm.get_total_exposure()
    if exposure == 200.0:
        pass_test("Exposure after hedge: $200")
    else:
        all_passed = fail_test("Exposure after hedge", 200.0, exposure)
    
    # Test P&L calculation
    # Bought 200 shares @ $0.50 ($100), now worth $0.60 = $120, P&L = +20%
    pnl_pct = pm.get_position_pnl_pct("BTC", "DOWN", 0.60)
    if 19.9 <= pnl_pct <= 20.1:  # Allow small float error
        pass_test(f"P&L calculation: +{pnl_pct:.1f}% (expected ~20%)")
    else:
        all_passed = fail_test("P&L calculation", "~20%", f"{pnl_pct:.1f}%")
    
    # Test closing position
    pnl = pm.close_position("BTC", "DOWN", 0.60)
    state = pm.get_state("BTC")
    if state == PositionState.LONG_UP:
        pass_test("After selling DOWN: LONG_UP")
    else:
        all_passed = fail_test("After selling DOWN", PositionState.LONG_UP, state)
    
    # Test P&L return value (bought 200 shares @ $0.50 = $100, sold @ $0.60 = $120)
    if 19.9 <= pnl <= 20.1:
        pass_test(f"Close returns P&L: ${pnl:.2f} (expected ~$20)")
    else:
        all_passed = fail_test("Close P&L return", "~$20", f"${pnl:.2f}")
    
    # Test cooldown
    can_trade = pm.can_cooldown_trade("BTC")
    if not can_trade:
        pass_test("Cooldown active immediately after exit")
    else:
        all_passed = fail_test("Cooldown active", False, can_trade)
    
    # Test clear_all
    pm.clear_all()
    state = pm.get_state("BTC")
    if state == PositionState.NONE:
        pass_test("After clear_all: NONE")
    else:
        all_passed = fail_test("After clear_all", PositionState.NONE, state)
    
    return all_passed


def verify_assets():
    """Verify required assets are configured."""
    section("9. Assets Configuration")
    all_passed = True
    
    required_assets = ["BTC", "ETH", "SOL"]
    
    for asset in required_assets:
        if asset in ASSETS:
            pass_test(f"Asset {asset} configured")
            # Verify it has required keys
            for key in ["binance", "polymarket", "coingecko"]:
                if key in ASSETS[asset]:
                    pass_test(f"  - {asset} has '{key}' mapping")
                else:
                    all_passed = fail_test(f"{asset} has '{key}'", "exists", "missing")
        else:
            all_passed = fail_test(f"Asset {asset}", "exists", "missing")
    
    return all_passed


def verify_state_transitions():
    """Verify complete state transition paths."""
    section("10. State Transition Paths (Full Scenario)")
    all_passed = True
    pm = PositionManager()
    sm = TradingStateMachine()
    
    print(f"\n  {Colors.YELLOW}Simulating: NONE → LONG_DOWN → profit → NONE → re-entry{Colors.RESET}")
    
    # Step 1: NONE, initial entry → BUY_DOWN
    state = pm.get_state("BTC")
    action = sm.decide(state, 0.0, 0.0, 55, 0.01, True, True)
    if action == Action.BUY_DOWN:
        pm.open_position("BTC", "DOWN", 0.50, 100)
        pass_test("Step 1: NONE → BUY_DOWN → LONG_DOWN")
    else:
        all_passed = fail_test("Step 1", Action.BUY_DOWN, action)
    
    # Step 2: LONG_DOWN, position profitable → SELL_DOWN
    state = pm.get_state("BTC")
    pnl_pct = pm.get_position_pnl_pct("BTC", "DOWN", 0.60)  # +20%
    action = sm.decide(state, -0.5, pnl_pct, 50, 0.01, True, False)
    if action == Action.SELL_DOWN:
        pm.close_position("BTC", "DOWN", 0.60)
        pass_test("Step 2: LONG_DOWN +20% → SELL_DOWN → NONE")
    else:
        all_passed = fail_test("Step 2", Action.SELL_DOWN, action)
    
    # Step 3: NONE, in cooldown → HOLD
    state = pm.get_state("BTC")
    can_trade = pm.can_cooldown_trade("BTC")
    action = sm.decide(state, -0.5, 0.0, 48, 0.01, can_trade, False)
    if action == Action.HOLD:
        pass_test("Step 3: NONE in cooldown → HOLD")
    else:
        all_passed = fail_test("Step 3", Action.HOLD, action)
    
    # Step 4: NONE, cooldown passed, price up → BUY_UP
    state = pm.get_state("BTC")
    action = sm.decide(state, 0.5, 0.0, 45, 0.01, True, False)  # Simulate cooldown passed
    if action == Action.BUY_UP:
        pm.open_position("BTC", "UP", 0.55, 100)
        pass_test("Step 4: NONE, trending UP → BUY_UP → LONG_UP")
    else:
        all_passed = fail_test("Step 4", Action.BUY_UP, action)
    
    print(f"\n  {Colors.YELLOW}Simulating: LONG_UP → losing → hedge → HEDGED → exit{Colors.RESET}")
    
    # Step 5: LONG_UP, position losing → hedge
    state = pm.get_state("BTC")
    pnl_pct = pm.get_position_pnl_pct("BTC", "UP", 0.45)  # ~-18%
    action = sm.decide(state, -0.5, pnl_pct, 40, 0.01, True, False)
    if action == Action.BUY_DOWN:
        pm.open_position("BTC", "DOWN", 0.55, 100)
        pass_test("Step 5: LONG_UP -18% → BUY_DOWN (hedge) → HEDGED")
    else:
        all_passed = fail_test("Step 5", Action.BUY_DOWN, action)
    
    # Step 6: HEDGED, near hour end, DOWN winning → SELL_UP
    state = pm.get_state("BTC")
    action = sm.decide(state, -0.5, 0.0, 8, 0.01, True, False)
    if action == Action.SELL_UP:
        pm.close_position("BTC", "UP", 0.40)
        pass_test("Step 6: HEDGED, DOWN winning → SELL_UP → LONG_DOWN")
    else:
        all_passed = fail_test("Step 6", Action.SELL_UP, action)
    
    # Final state should be LONG_DOWN
    state = pm.get_state("BTC")
    if state == PositionState.LONG_DOWN:
        pass_test("Final state: LONG_DOWN (holding winner)")
    else:
        all_passed = fail_test("Final state", PositionState.LONG_DOWN, state)
    
    return all_passed


def main():
    print(f"\n{Colors.BOLD}{'='*60}")
    print("HOURLY TRADING BOT - PLAN VERIFICATION")
    print(f"{'='*60}{Colors.RESET}")
    
    results = []
    
    results.append(("Configuration Parameters", verify_configuration_parameters()))
    results.append(("State Machine States", verify_state_machine_states()))
    results.append(("Actions", verify_actions()))
    results.append(("Entry Rules", verify_entry_rules()))
    results.append(("Profit-Take Rules", verify_profit_take_rules()))
    results.append(("Hedge Rules", verify_hedge_rules()))
    results.append(("Hedged State", verify_hedged_state()))
    results.append(("Position Manager", verify_position_manager()))
    results.append(("Assets Configuration", verify_assets()))
    results.append(("State Transitions", verify_state_transitions()))
    
    # Summary
    print(f"\n{Colors.BOLD}{'='*60}")
    print("SUMMARY")
    print(f"{'='*60}{Colors.RESET}")
    
    passed = sum(1 for _, r in results if r)
    total = len(results)
    
    for name, result in results:
        status = f"{Colors.GREEN}PASS{Colors.RESET}" if result else f"{Colors.RED}FAIL{Colors.RESET}"
        print(f"  {status} - {name}")
    
    print(f"\n{Colors.BOLD}Result: {passed}/{total} test groups passed{Colors.RESET}")
    
    if passed == total:
        print(f"\n{Colors.GREEN}✓ Bot implementation matches plan specifications!{Colors.RESET}\n")
        return 0
    else:
        print(f"\n{Colors.RED}✗ Some tests failed - review implementation{Colors.RESET}\n")
        return 1


if __name__ == "__main__":
    sys.exit(main())
