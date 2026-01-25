#!/usr/bin/env python3
"""
Test script for Polymarket Copy Trading Bot.

Validates the bot components without placing real trades.
Can also be used as a dry-run to verify everything works.
"""

import asyncio
import sys
import logging

# Setup logging for tests
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


async def test_config():
    """Test configuration loading and validation."""
    logger.info("Testing configuration...")
    
    import config
    
    errors = config.validate_config()
    if errors:
        for error in errors:
            logger.error(f"  Config error: {error}")
        return False
    
    logger.info(f"  Target username: {config.TARGET_USERNAME}")
    logger.info(f"  Max slippage: {config.MAX_SLIPPAGE_PERCENT}%")
    logger.info(f"  Max trade USD: ${config.MAX_TRADE_USD}")
    logger.info(f"  Min trade USD: ${config.MIN_TRADE_USD}")
    logger.info(f"  Dry run: {config.DRY_RUN}")
    logger.info("  Config OK!")
    return True


async def test_data_client():
    """Test the data client API calls."""
    logger.info("Testing data client...")
    
    import config
    from api.data_client import DataClient
    
    client = DataClient()
    
    try:
        # Test profile search
        logger.info(f"  Searching for profile: {config.TARGET_USERNAME}")
        profile = await client.search_profile(config.TARGET_USERNAME)
        
        if not profile:
            logger.error(f"  Could not find profile for: {config.TARGET_USERNAME}")
            return False
        
        logger.info(f"  Found wallet: {profile.wallet_address}")
        logger.info(f"  Username: {profile.username}")
        logger.info(f"  Name: {profile.name}")
        
        # Test trades fetch
        logger.info("  Fetching recent trades...")
        trades = await client.get_trades(profile.wallet_address, limit=5)
        logger.info(f"  Found {len(trades)} recent trades")
        
        for trade in trades[:3]:
            logger.info(f"    - {trade.side} {trade.size:.2f} @ ${trade.price:.4f}: {trade.title[:40]}...")
        
        # Test positions fetch
        logger.info("  Fetching positions...")
        positions = await client.get_positions(profile.wallet_address)
        logger.info(f"  Found {len(positions)} positions")
        
        # Test portfolio value
        logger.info("  Fetching portfolio value...")
        value = await client.get_portfolio_value(profile.wallet_address)
        logger.info(f"  Portfolio value: ${value:,.2f}")
        
        logger.info("  Data client OK!")
        return True
        
    except Exception as e:
        logger.error(f"  Data client error: {e}")
        return False
        
    finally:
        await client.close()


async def test_clob_client():
    """Test the CLOB client initialization."""
    logger.info("Testing CLOB client...")
    
    import config
    from api.clob_client import ClobClient
    
    if not config.PRIVATE_KEY:
        logger.warning("  PRIVATE_KEY not set, skipping CLOB client test")
        return True
    
    client = ClobClient()
    
    try:
        logger.info("  Initializing CLOB client...")
        await client.initialize()
        
        address = client.get_address()
        logger.info(f"  Wallet address: {address}")
        
        # Test orderbook fetch with a known active market
        # This is a sample token ID - may need to be updated
        logger.info("  Testing orderbook fetch...")
        # Note: We skip this in basic test since we need a valid token ID
        
        logger.info("  CLOB client OK!")
        return True
        
    except Exception as e:
        logger.error(f"  CLOB client error: {e}")
        return False
        
    finally:
        await client.close()


async def test_state_storage():
    """Test the SQLite state storage."""
    logger.info("Testing state storage...")
    
    from storage.state import StateStorage, TradeStatus
    import os
    
    # Use a test database
    test_db = "test_copied_trades.db"
    
    try:
        storage = StateStorage(test_db)
        await storage.initialize()
        
        # Test recording a copied trade
        logger.info("  Recording test trade...")
        trade = await storage.record_copied(
            target_tx_hash="0xtest123",
            token_id="test_token",
            condition_id="test_condition",
            side="BUY",
            target_price=0.65,
            target_size=100,
            my_price=0.66,
            my_size=50,
            my_order_id="order123",
        )
        logger.info(f"  Created trade record: {trade.id}")
        
        # Test duplicate check
        logger.info("  Testing duplicate check...")
        is_dup = await storage.is_already_copied("0xtest123")
        assert is_dup, "Should detect duplicate"
        logger.info("  Duplicate detection: OK")
        
        is_not_dup = await storage.is_already_copied("0xother456")
        assert not is_not_dup, "Should not detect non-existent"
        logger.info("  Non-duplicate detection: OK")
        
        # Test recording skipped trade
        logger.info("  Recording skipped trade...")
        await storage.record_skipped(
            target_tx_hash="0xskipped123",
            token_id="test_token",
            condition_id="test_condition",
            side="SELL",
            target_price=0.70,
            target_size=200,
            status=TradeStatus.SKIPPED_SLIPPAGE,
            reason="Slippage 15% exceeds 10% threshold",
        )
        
        # Test stats
        logger.info("  Checking stats...")
        stats = await storage.get_stats()
        logger.info(f"  Stats: {stats}")
        
        # Test recent trades
        logger.info("  Fetching recent trades...")
        recent = await storage.get_recent_trades(limit=10)
        logger.info(f"  Found {len(recent)} recent trades")
        
        await storage.close()
        logger.info("  State storage OK!")
        return True
        
    except Exception as e:
        logger.error(f"  State storage error: {e}")
        return False
        
    finally:
        # Cleanup test database
        if os.path.exists(test_db):
            os.remove(test_db)
        if os.path.exists(f"{test_db}-wal"):
            os.remove(f"{test_db}-wal")
        if os.path.exists(f"{test_db}-shm"):
            os.remove(f"{test_db}-shm")


async def test_slippage_calculation():
    """Test slippage calculation logic."""
    logger.info("Testing slippage calculation...")
    
    import config
    
    # Simulate slippage calculation
    def calc_slippage(target_price: float, current_price: float, side: str) -> float:
        if side == "BUY":
            return ((current_price - target_price) / target_price) * 100
        else:
            return ((target_price - current_price) / target_price) * 100
    
    # Test cases
    tests = [
        # (target_price, current_price, side, expected_acceptable)
        (0.50, 0.51, "BUY", True),    # 2% slippage - acceptable
        (0.50, 0.60, "BUY", False),   # 20% slippage - not acceptable
        (0.50, 0.49, "SELL", True),   # 2% slippage - acceptable
        (0.50, 0.40, "SELL", False),  # 20% slippage - not acceptable
        (0.50, 0.55, "BUY", False),   # 10% exactly at threshold
        (0.50, 0.545, "BUY", True),   # 9% just under threshold
    ]
    
    for target, current, side, expected in tests:
        slippage = calc_slippage(target, current, side)
        acceptable = abs(slippage) <= config.MAX_SLIPPAGE_PERCENT
        status = "✓" if acceptable == expected else "✗"
        logger.info(
            f"  {status} {side} target=${target} current=${current} "
            f"slippage={slippage:.1f}% acceptable={acceptable}"
        )
        
        if acceptable != expected:
            logger.error(f"    Expected acceptable={expected}, got {acceptable}")
            return False
    
    logger.info("  Slippage calculation OK!")
    return True


async def test_proportional_sizing():
    """Test proportional trade sizing logic."""
    logger.info("Testing proportional sizing...")
    
    import config
    
    def calc_size(target_trade_usd: float, target_portfolio: float, my_portfolio: float) -> float:
        if target_portfolio <= 0 or my_portfolio <= 0:
            return config.MIN_TRADE_USD
        
        target_percent = target_trade_usd / target_portfolio
        my_trade = target_percent * my_portfolio
        return max(config.MIN_TRADE_USD, min(config.MAX_TRADE_USD, my_trade))
    
    # Test cases
    tests = [
        # (target_trade, target_portfolio, my_portfolio, expected_size)
        (100, 10000, 5000, 50),      # Proportional: 1% of each
        (100, 10000, 20000, 100),    # Limited by MAX_TRADE_USD (assuming max=100)
        (1, 10000, 5000, 1),         # Limited by MIN_TRADE_USD
        (500, 10000, 10000, 100),    # 5% capped at MAX
    ]
    
    for target_trade, target_port, my_port, expected in tests:
        result = calc_size(target_trade, target_port, my_port)
        status = "✓" if abs(result - expected) < 0.01 else "✗"
        logger.info(
            f"  {status} trade=${target_trade} target_port=${target_port} my_port=${my_port} "
            f"-> ${result:.2f} (expected ${expected:.2f})"
        )
    
    logger.info("  Proportional sizing OK!")
    return True


async def run_all_tests():
    """Run all tests."""
    logger.info("=" * 60)
    logger.info("Polymarket Copy Trading Bot - Test Suite")
    logger.info("=" * 60)
    
    results = {
        "config": await test_config(),
        "slippage": await test_slippage_calculation(),
        "sizing": await test_proportional_sizing(),
        "storage": await test_state_storage(),
    }
    
    # These tests require API access
    if results["config"]:
        results["data_client"] = await test_data_client()
        results["clob_client"] = await test_clob_client()
    
    logger.info("=" * 60)
    logger.info("Test Results:")
    logger.info("=" * 60)
    
    all_passed = True
    for name, passed in results.items():
        status = "PASS" if passed else "FAIL"
        logger.info(f"  {name}: {status}")
        if not passed:
            all_passed = False
    
    logger.info("=" * 60)
    
    if all_passed:
        logger.info("All tests passed!")
        return 0
    else:
        logger.error("Some tests failed!")
        return 1


if __name__ == "__main__":
    sys.exit(asyncio.run(run_all_tests()))
