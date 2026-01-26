#!/usr/bin/env python3
"""
Polymarket Copy Trading Bot

Main entry point that orchestrates all components:
- Resolves target username to wallet address
- Initializes CLOB client with your credentials
- Monitors target for new trades
- Copies trades proportionally with slippage protection
"""

import asyncio
import logging
import signal
import sys
from typing import Optional

import config
from api.data_client import DataClient, Trade
from api.clob_client import ClobClient
from storage.state import StateStorage
from copier.copy_engine import CopyEngine
from monitor.trade_monitor import TradeMonitorWithBackoff

# Configure logging
logging.basicConfig(
    level=getattr(logging, config.LOG_LEVEL.upper()),
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


class CopyTradingBot:
    """
    Main bot orchestrator.
    
    Coordinates all components and handles lifecycle.
    """
    
    def __init__(self):
        self._data_client: Optional[DataClient] = None
        self._clob_client: Optional[ClobClient] = None
        self._state: Optional[StateStorage] = None
        self._copy_engine: Optional[CopyEngine] = None
        self._monitor: Optional[TradeMonitorWithBackoff] = None
        
        self._target_wallet: Optional[str] = None
        self._my_wallet: Optional[str] = None
        self._shutdown_event = asyncio.Event()
    
    async def initialize(self) -> bool:
        """
        Initialize all components.
        
        Returns:
            True if initialization successful, False otherwise
        """
        logger.info("=" * 60)
        logger.info("Polymarket Copy Trading Bot")
        logger.info("=" * 60)
        
        # Validate configuration
        errors = config.validate_config()
        if errors:
            for error in errors:
                logger.error(f"Config error: {error}")
            return False
        
        # Log configuration
        logger.info(f"Target wallet: {config.TARGET_WALLET}")
        logger.info(f"Max slippage: {config.MAX_SLIPPAGE_PERCENT}%")
        logger.info(f"Max trade USD: ${config.MAX_TRADE_USD}")
        logger.info(f"Min trade USD: ${config.MIN_TRADE_USD}")
        logger.info(f"Poll interval: {config.POLL_INTERVAL_SECONDS}s")
        logger.info(f"Dry run mode: {config.DRY_RUN}")
        
        if config.DRY_RUN:
            logger.warning("=" * 60)
            logger.warning("DRY RUN MODE - No real trades will be executed")
            logger.warning("=" * 60)
        
        # Initialize data client
        logger.info("Initializing data client...")
        self._data_client = DataClient()
        
        # Use target wallet from config
        self._target_wallet = config.TARGET_WALLET
        logger.info(f"Target wallet: {self._target_wallet}")
        
        # Initialize CLOB client
        logger.info("Initializing CLOB client...")
        self._clob_client = ClobClient()
        
        try:
            await self._clob_client.initialize()
        except Exception as e:
            logger.error(f"Failed to initialize CLOB client: {e}")
            return False
        
        self._my_wallet = self._clob_client.get_address()
        logger.info(f"Your wallet: {self._my_wallet}")
        
        # Get portfolio values
        try:
            target_value = await self._data_client.get_portfolio_value(self._target_wallet)
            my_value = await self._data_client.get_portfolio_value(self._my_wallet)
            logger.info(f"Target portfolio value: ${target_value:,.2f}")
            logger.info(f"Your portfolio value: ${my_value:,.2f}")
        except Exception as e:
            logger.warning(f"Could not fetch portfolio values: {e}")
        
        # Initialize state storage
        logger.info("Initializing state storage...")
        self._state = StateStorage()
        await self._state.initialize()
        
        # Get stats from previous runs
        stats = await self._state.get_stats()
        logger.info(f"Previous session stats: {stats}")
        
        # Initialize copy engine
        logger.info("Initializing copy engine...")
        self._copy_engine = CopyEngine(
            data_client=self._data_client,
            clob_client=self._clob_client,
            state=self._state,
        )
        
        # Initialize trade monitor
        logger.info("Initializing trade monitor...")
        self._monitor = TradeMonitorWithBackoff(
            data_client=self._data_client,
            state=self._state,
            target_wallet=self._target_wallet,
            on_new_trade=self._handle_new_trade,
        )
        
        logger.info("Initialization complete!")
        return True
    
    async def _handle_new_trade(self, trade: Trade) -> None:
        """
        Callback invoked when a new trade is detected.
        
        Args:
            trade: The new trade from the target
        """
        logger.info(f"Processing new trade: {trade.tx_hash[:16]}...")
        
        await self._copy_engine.process_trade(
            trade=trade,
            target_wallet=self._target_wallet,
            my_wallet=self._my_wallet,
        )
    
    async def run(self) -> None:
        """Run the bot main loop."""
        if not await self.initialize():
            logger.error("Initialization failed, exiting")
            return
        
        logger.info("=" * 60)
        logger.info("Bot started - monitoring for trades...")
        logger.info("Press Ctrl+C to stop")
        logger.info("=" * 60)
        
        # Start the monitor
        monitor_task = asyncio.create_task(self._monitor.start())
        
        # Wait for shutdown signal
        await self._shutdown_event.wait()
        
        # Stop the monitor
        await self._monitor.stop()
        monitor_task.cancel()
        
        try:
            await monitor_task
        except asyncio.CancelledError:
            pass
    
    async def shutdown(self) -> None:
        """Graceful shutdown."""
        logger.info("Shutting down...")
        
        self._shutdown_event.set()
        
        # Close clients
        if self._data_client:
            await self._data_client.close()
        
        if self._clob_client:
            await self._clob_client.close()
        
        if self._state:
            await self._state.close()
        
        logger.info("Shutdown complete")
    
    def handle_signal(self, sig: signal.Signals) -> None:
        """Handle OS signals for graceful shutdown."""
        logger.info(f"Received signal {sig.name}")
        self._shutdown_event.set()


async def main():
    """Main entry point."""
    bot = CopyTradingBot()
    
    # Setup signal handlers
    loop = asyncio.get_running_loop()
    
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(
            sig,
            lambda s=sig: bot.handle_signal(s)
        )
    
    try:
        await bot.run()
    finally:
        await bot.shutdown()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        sys.exit(1)
