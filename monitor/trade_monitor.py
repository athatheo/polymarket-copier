"""
Trade monitor that polls target account for new trades.

Continuously watches a target account and triggers copy operations.
"""

import asyncio
import logging
from typing import Optional, Callable, Awaitable
from datetime import datetime

from api.data_client import DataClient, Trade
from storage.state import StateStorage
import config

logger = logging.getLogger(__name__)


class TradeMonitor:
    """
    Monitors a target account for new trades.
    
    Polls the target's trade history and invokes a callback
    for each new trade detected.
    """
    
    def __init__(
        self,
        data_client: DataClient,
        state: StateStorage,
        target_wallet: str,
        on_new_trade: Callable[[Trade], Awaitable[None]],
    ):
        """
        Initialize the trade monitor.
        
        Args:
            data_client: Client for fetching trade data
            state: Storage for tracking processed trades
            target_wallet: Wallet address to monitor
            on_new_trade: Async callback invoked for each new trade
        """
        self._data = data_client
        self._state = state
        self._target_wallet = target_wallet
        self._on_new_trade = on_new_trade
        
        self._running = False
        self._last_check_timestamp: Optional[int] = None
        self._poll_count = 0
        self._trades_found = 0
    
    async def start(self) -> None:
        """Start the monitoring loop."""
        self._running = True
        
        # Get last processed timestamp to avoid reprocessing
        self._last_check_timestamp = await self._state.get_last_processed_timestamp()
        
        if self._last_check_timestamp:
            logger.info(
                f"Resuming from last processed timestamp: "
                f"{datetime.fromtimestamp(self._last_check_timestamp)}"
            )
        else:
            # Start from now to avoid processing historical trades
            self._last_check_timestamp = int(datetime.utcnow().timestamp())
            logger.info("Starting fresh - will only process new trades from now")
        
        logger.info(
            f"Starting trade monitor for wallet {self._target_wallet}, "
            f"polling every {config.POLL_INTERVAL_SECONDS}s"
        )
        
        while self._running:
            try:
                await self._poll_once()
            except Exception as e:
                logger.error(f"Error in polling loop: {e}", exc_info=True)
            
            await asyncio.sleep(config.POLL_INTERVAL_SECONDS)
    
    async def stop(self) -> None:
        """Stop the monitoring loop."""
        logger.info("Stopping trade monitor...")
        self._running = False
    
    async def _poll_once(self) -> None:
        """Execute a single poll for new trades."""
        self._poll_count += 1
        
        try:
            trades = await self._data.get_trades(
                wallet=self._target_wallet,
                since_timestamp=self._last_check_timestamp,
                limit=50,
            )
        except Exception as e:
            logger.error(f"Failed to fetch trades: {e}")
            return
        
        if not trades:
            logger.debug(f"Poll #{self._poll_count}: No new trades")
            return
        
        logger.info(f"Poll #{self._poll_count}: Found {len(trades)} new trades")
        
        # Process trades oldest first (reverse since API returns newest first)
        trades.sort(key=lambda t: t.timestamp)
        
        for trade in trades:
            # Skip if already processed
            if await self._state.is_already_copied(trade.tx_hash):
                logger.debug(f"Trade {trade.tx_hash} already processed")
                continue
            
            self._trades_found += 1
            
            logger.info(
                f"New trade detected: {trade.side} {trade.size:.4f} shares "
                f"@ ${trade.price:.4f} for {trade.title} - {trade.outcome}"
            )
            
            # Invoke callback
            try:
                await self._on_new_trade(trade)
            except Exception as e:
                logger.error(f"Error processing trade {trade.tx_hash}: {e}", exc_info=True)
            
            # Update last check timestamp
            self._last_check_timestamp = max(
                self._last_check_timestamp or 0,
                trade.timestamp
            )
    
    def get_stats(self) -> dict:
        """Get monitoring statistics."""
        return {
            "running": self._running,
            "poll_count": self._poll_count,
            "trades_found": self._trades_found,
            "target_wallet": self._target_wallet,
            "poll_interval": config.POLL_INTERVAL_SECONDS,
            "last_check": datetime.fromtimestamp(self._last_check_timestamp).isoformat()
            if self._last_check_timestamp else None,
        }


class TradeMonitorWithBackoff(TradeMonitor):
    """
    Trade monitor with exponential backoff on errors.
    
    Automatically slows down polling when errors occur
    and speeds up when things are stable.
    """
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._consecutive_errors = 0
        self._max_backoff = 300  # Max 5 minutes between polls on errors
    
    def _get_poll_interval(self) -> int:
        """Get current poll interval with backoff applied."""
        if self._consecutive_errors == 0:
            return config.POLL_INTERVAL_SECONDS
        
        # Exponential backoff: 2^errors * base_interval, capped at max
        backoff = min(
            (2 ** self._consecutive_errors) * config.POLL_INTERVAL_SECONDS,
            self._max_backoff
        )
        return int(backoff)
    
    async def start(self) -> None:
        """Start the monitoring loop with backoff."""
        self._running = True
        
        self._last_check_timestamp = await self._state.get_last_processed_timestamp()
        
        if self._last_check_timestamp:
            logger.info(
                f"Resuming from last processed timestamp: "
                f"{datetime.fromtimestamp(self._last_check_timestamp)}"
            )
        else:
            self._last_check_timestamp = int(datetime.utcnow().timestamp())
            logger.info("Starting fresh - will only process new trades from now")
        
        logger.info(
            f"Starting trade monitor (with backoff) for wallet {self._target_wallet}"
        )
        
        while self._running:
            try:
                await self._poll_once()
                self._consecutive_errors = 0  # Reset on success
            except Exception as e:
                self._consecutive_errors += 1
                logger.error(
                    f"Error in polling loop (consecutive: {self._consecutive_errors}): {e}",
                    exc_info=True
                )
            
            interval = self._get_poll_interval()
            if interval != config.POLL_INTERVAL_SECONDS:
                logger.warning(f"Backoff active: next poll in {interval}s")
            
            await asyncio.sleep(interval)
