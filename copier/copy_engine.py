"""
Copy trading engine.

Handles:
- Slippage checking
- Proportional trade sizing
- Trade execution coordination
"""

import logging
from dataclasses import dataclass
from typing import Optional, Tuple

from api.data_client import DataClient, Trade
from api.clob_client import ClobClient, OrderResult
from storage.state import StateStorage, TradeStatus
import config

logger = logging.getLogger(__name__)


@dataclass
class SlippageCheckResult:
    """Result of a slippage check."""
    is_acceptable: bool
    target_price: float
    current_price: Optional[float]
    slippage_percent: float
    reason: str


@dataclass
class CopyDecision:
    """Decision about whether and how to copy a trade."""
    should_copy: bool
    trade: Trade
    my_size_usd: float
    my_size_shares: float
    current_price: float
    slippage_percent: float
    skip_reason: Optional[str] = None
    skip_status: Optional[TradeStatus] = None


class CopyEngine:
    """
    Engine that decides whether to copy trades and executes them.
    
    Responsibilities:
    - Check slippage against threshold
    - Calculate proportional trade size
    - Apply safety limits (min/max)
    - Execute trades via CLOB
    - Record results in state storage
    """
    
    def __init__(
        self,
        data_client: DataClient,
        clob_client: ClobClient,
        state: StateStorage,
    ):
        self._data = data_client
        self._clob = clob_client
        self._state = state
    
    async def check_slippage(
        self, 
        target_price: float, 
        token_id: str, 
        side: str,
    ) -> SlippageCheckResult:
        """
        Check if current market price is within acceptable slippage.
        
        For BUY orders: compare target price to current best ASK
        For SELL orders: compare target price to current best BID
        
        Args:
            target_price: The price the target traded at
            token_id: The token/asset ID
            side: "BUY" or "SELL"
            
        Returns:
            SlippageCheckResult with pass/fail and details
        """
        try:
            orderbook = await self._clob.get_orderbook(token_id)
        except Exception as e:
            logger.error(f"Failed to get orderbook: {e}")
            return SlippageCheckResult(
                is_acceptable=False,
                target_price=target_price,
                current_price=None,
                slippage_percent=100.0,
                reason=f"Failed to fetch orderbook: {e}",
            )
        
        # Get the relevant price based on order side
        if side.upper() == "BUY":
            current_price = orderbook.best_ask
            price_type = "ask"
        else:
            current_price = orderbook.best_bid
            price_type = "bid"
        
        # Check if orderbook has liquidity
        if current_price is None:
            return SlippageCheckResult(
                is_acceptable=False,
                target_price=target_price,
                current_price=None,
                slippage_percent=100.0,
                reason=f"No {price_type} liquidity in orderbook",
            )
        
        # Calculate slippage percentage
        # For BUY: positive slippage = price went up (bad)
        # For SELL: positive slippage = price went down (bad)
        if side.upper() == "BUY":
            # Buying: if current ask > target price, that's slippage
            slippage_pct = ((current_price - target_price) / target_price) * 100
        else:
            # Selling: if current bid < target price, that's slippage
            slippage_pct = ((target_price - current_price) / target_price) * 100
        
        # Absolute slippage for threshold comparison
        abs_slippage = abs(slippage_pct)
        
        is_acceptable = abs_slippage <= config.MAX_SLIPPAGE_PERCENT
        
        if is_acceptable:
            reason = f"Slippage {abs_slippage:.2f}% within {config.MAX_SLIPPAGE_PERCENT}% threshold"
        else:
            reason = f"Slippage {abs_slippage:.2f}% exceeds {config.MAX_SLIPPAGE_PERCENT}% threshold"
        
        return SlippageCheckResult(
            is_acceptable=is_acceptable,
            target_price=target_price,
            current_price=current_price,
            slippage_percent=abs_slippage,
            reason=reason,
        )
    
    def calculate_proportional_size(
        self,
        target_trade_usd: float,
        target_portfolio_usd: float,
        my_portfolio_usd: float,
    ) -> float:
        """
        Calculate the proportional trade size for copying.
        
        Matches the same portfolio percentage as the target.
        Example: Target trades $100 with $10k portfolio (1%)
                 You have $5k portfolio -> trade $50 (1%)
        
        Args:
            target_trade_usd: Value of target's trade in USD
            target_portfolio_usd: Total value of target's portfolio
            my_portfolio_usd: Total value of your portfolio
            
        Returns:
            Your trade size in USD, bounded by min/max limits
        """
        if target_portfolio_usd <= 0:
            logger.warning("Target portfolio value is zero or negative")
            return config.MIN_TRADE_USD
        
        if my_portfolio_usd <= 0:
            logger.warning("Your portfolio value is zero or negative")
            return config.MIN_TRADE_USD
        
        # Calculate the percentage of portfolio the target used
        target_percent = target_trade_usd / target_portfolio_usd
        
        # Apply the same percentage to your portfolio
        my_trade_usd = target_percent * my_portfolio_usd
        
        # Apply safety bounds
        original_size = my_trade_usd
        my_trade_usd = max(config.MIN_TRADE_USD, min(config.MAX_TRADE_USD, my_trade_usd))
        
        if my_trade_usd != original_size:
            logger.info(
                f"Trade size adjusted from ${original_size:.2f} to ${my_trade_usd:.2f} "
                f"(min=${config.MIN_TRADE_USD}, max=${config.MAX_TRADE_USD})"
            )
        
        return my_trade_usd
    
    async def evaluate_trade(
        self,
        trade: Trade,
        target_wallet: str,
        my_wallet: str,
    ) -> CopyDecision:
        """
        Evaluate whether a trade should be copied.
        
        Performs all checks and calculates sizing.
        
        Args:
            trade: The target's trade to potentially copy
            target_wallet: Target's wallet address
            my_wallet: Your wallet address
            
        Returns:
            CopyDecision with all details
        """
        # 0. For SELL trades, check if we have the position
        if trade.side.upper() == "SELL":
            try:
                my_positions = await self._data.get_positions(my_wallet)
                has_position = any(p.token_id == trade.token_id and p.size > 0 for p in my_positions)
                
                if not has_position:
                    logger.info(f"Skipping SELL trade - we don't own this token: {trade.title}")
                    return CopyDecision(
                        should_copy=False,
                        trade=trade,
                        my_size_usd=0,
                        my_size_shares=0,
                        current_price=trade.price,
                        slippage_percent=0,
                        skip_reason="Cannot SELL - we don't own this position",
                        skip_status=TradeStatus.SKIPPED_NO_POSITION,
                    )
            except Exception as e:
                logger.warning(f"Failed to check positions for SELL: {e}")
                return CopyDecision(
                    should_copy=False,
                    trade=trade,
                    my_size_usd=0,
                    my_size_shares=0,
                    current_price=trade.price,
                    slippage_percent=0,
                    skip_reason=f"Failed to check positions: {e}",
                    skip_status=TradeStatus.SKIPPED_ERROR,
                )
        
        # 1. Check slippage
        slippage_result = await self.check_slippage(
            target_price=trade.price,
            token_id=trade.token_id,
            side=trade.side,
        )
        
        if not slippage_result.is_acceptable:
            return CopyDecision(
                should_copy=False,
                trade=trade,
                my_size_usd=0,
                my_size_shares=0,
                current_price=slippage_result.current_price or trade.price,
                slippage_percent=slippage_result.slippage_percent,
                skip_reason=slippage_result.reason,
                skip_status=TradeStatus.SKIPPED_SLIPPAGE,
            )
        
        # 2. Get portfolio values for proportional sizing
        try:
            target_portfolio = await self._data.get_portfolio_value(target_wallet)
            my_portfolio = await self._data.get_portfolio_value(my_wallet)
        except Exception as e:
            logger.error(f"Failed to get portfolio values: {e}")
            return CopyDecision(
                should_copy=False,
                trade=trade,
                my_size_usd=0,
                my_size_shares=0,
                current_price=slippage_result.current_price or trade.price,
                slippage_percent=slippage_result.slippage_percent,
                skip_reason=f"Failed to get portfolio values: {e}",
                skip_status=TradeStatus.SKIPPED_ERROR,
            )
        
        # 3. Calculate proportional size
        target_trade_usd = trade.size * trade.price
        my_size_usd = self.calculate_proportional_size(
            target_trade_usd=target_trade_usd,
            target_portfolio_usd=target_portfolio,
            my_portfolio_usd=my_portfolio,
        )
        
        # 4. Check if size is too small
        if my_size_usd < config.MIN_TRADE_USD:
            return CopyDecision(
                should_copy=False,
                trade=trade,
                my_size_usd=my_size_usd,
                my_size_shares=0,
                current_price=slippage_result.current_price,
                slippage_percent=slippage_result.slippage_percent,
                skip_reason=f"Trade size ${my_size_usd:.2f} below minimum ${config.MIN_TRADE_USD}",
                skip_status=TradeStatus.SKIPPED_SIZE,
            )
        
        # 5. Convert USD to shares at current price
        current_price = slippage_result.current_price
        my_size_shares = my_size_usd / current_price
        
        return CopyDecision(
            should_copy=True,
            trade=trade,
            my_size_usd=my_size_usd,
            my_size_shares=my_size_shares,
            current_price=current_price,
            slippage_percent=slippage_result.slippage_percent,
        )
    
    async def execute_copy(
        self,
        decision: CopyDecision,
    ) -> Optional[OrderResult]:
        """
        Execute a copy trade.
        
        Args:
            decision: The evaluated copy decision
            
        Returns:
            OrderResult if executed, None if dry run
        """
        trade = decision.trade
        
        if config.DRY_RUN:
            logger.info(
                f"[DRY RUN] Would {trade.side} {decision.my_size_shares:.4f} shares "
                f"@ ${decision.current_price:.4f} (${decision.my_size_usd:.2f} USD) "
                f"for market: {trade.title} - {trade.outcome}"
            )
            # Record as copied even in dry run for state tracking
            await self._state.record_copied(
                target_tx_hash=trade.tx_hash,
                token_id=trade.token_id,
                condition_id=trade.condition_id,
                side=trade.side,
                target_price=trade.price,
                target_size=trade.size,
                my_price=decision.current_price,
                my_size=decision.my_size_shares,
                my_order_id="DRY_RUN",
            )
            return None
        
        # Execute the order
        logger.info(
            f"Executing {trade.side} {decision.my_size_shares:.4f} shares "
            f"@ ${decision.current_price:.4f} for: {trade.title} - {trade.outcome}"
        )
        
        result = await self._clob.place_order(
            token_id=trade.token_id,
            side=trade.side,
            price=decision.current_price,
            size=decision.my_size_shares,
        )
        
        if result.success:
            await self._state.record_copied(
                target_tx_hash=trade.tx_hash,
                token_id=trade.token_id,
                condition_id=trade.condition_id,
                side=trade.side,
                target_price=trade.price,
                target_size=trade.size,
                my_price=decision.current_price,
                my_size=decision.my_size_shares,
                my_order_id=result.order_id,
            )
            logger.info(f"Order placed successfully: {result.order_id}")
        else:
            await self._state.record_skipped(
                target_tx_hash=trade.tx_hash,
                token_id=trade.token_id,
                condition_id=trade.condition_id,
                side=trade.side,
                target_price=trade.price,
                target_size=trade.size,
                status=TradeStatus.SKIPPED_ERROR,
                reason=f"Order failed: {result.error}",
            )
            logger.error(f"Order failed: {result.error}")
        
        return result
    
    async def process_trade(
        self,
        trade: Trade,
        target_wallet: str,
        my_wallet: str,
    ) -> bool:
        """
        Process a single trade from start to finish.
        
        Args:
            trade: The target's trade
            target_wallet: Target's wallet address
            my_wallet: Your wallet address
            
        Returns:
            True if trade was copied or appropriately skipped
        """
        # Check if already processed
        if await self._state.is_already_copied(trade.tx_hash):
            logger.debug(f"Trade {trade.tx_hash} already processed, skipping")
            return True
        
        logger.info(
            f"Processing trade: {trade.side} {trade.size:.4f} @ ${trade.price:.4f} "
            f"for {trade.title} - {trade.outcome}"
        )
        
        # Evaluate the trade
        decision = await self.evaluate_trade(trade, target_wallet, my_wallet)
        
        if not decision.should_copy:
            # Record the skip
            await self._state.record_skipped(
                target_tx_hash=trade.tx_hash,
                token_id=trade.token_id,
                condition_id=trade.condition_id,
                side=trade.side,
                target_price=trade.price,
                target_size=trade.size,
                status=decision.skip_status,
                reason=decision.skip_reason,
            )
            logger.warning(f"Skipping trade: {decision.skip_reason}")
            return True
        
        # Execute the copy
        await self.execute_copy(decision)
        return True
