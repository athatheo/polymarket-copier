"""
CLOB (Central Limit Order Book) client for Polymarket.

Handles:
- API key creation/derivation
- Order book fetching (for slippage checks)
- Order placement

Order types:
- FAK (Fill-And-Kill): Fills as much as immediately available, cancels the rest.
  Used for thin markets where FOK would fail entirely. Partial fills are better
  than no fills.
"""

import asyncio
import logging
from dataclasses import dataclass
from typing import Optional

from py_clob_client.client import ClobClient as PyClobClient
from py_clob_client.clob_types import MarketOrderArgs, OrderType
from py_clob_client.order_builder.constants import BUY, SELL
from eth_account import Account

import config

logger = logging.getLogger(__name__)


@dataclass
class OrderBook:
    """Simplified order book with best bid/ask."""
    token_id: str
    best_bid: Optional[float]  # Highest buy price
    best_ask: Optional[float]  # Lowest sell price
    bid_size: float
    ask_size: float


@dataclass
class OrderResult:
    """Result of placing an order."""
    order_id: str
    success: bool
    filled_size: float  # Shares filled (for both BUY and SELL)
    filled_price: float
    error: Optional[str] = None


class ClobClient:
    """
    Client for Polymarket's CLOB trading API.
    
    Wraps py-clob-client with a simpler interface.
    """
    
    def __init__(self):
        self._client: Optional[PyClobClient] = None
        self._address: Optional[str] = None
        self._initialized = False
    
    async def initialize(self) -> None:
        """
        Initialize the CLOB client with credentials.
        
        Creates or derives API keys from the private key.
        """
        if self._initialized:
            return
        
        if not config.PRIVATE_KEY:
            raise ValueError("PRIVATE_KEY not configured")
        
        # Derive address from private key
        account = Account.from_key(config.PRIVATE_KEY)
        self._address = account.address
        
        logger.info(f"Initializing CLOB client for wallet: {self._address}")
        
        # Create the CLOB client
        self._client = PyClobClient(
            host=config.CLOB_API_URL,
            chain_id=config.CHAIN_ID,
            key=config.PRIVATE_KEY,
        )
        
        # Create or derive API credentials
        try:
            api_creds = self._client.create_or_derive_api_creds()
            self._client.set_api_creds(api_creds)
            logger.info("API credentials initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize API credentials: {e}")
            raise
        
        self._initialized = True
    
    def get_address(self) -> str:
        """Get the wallet address."""
        if not self._address:
            raise RuntimeError("Client not initialized")
        return self._address
    
    async def get_orderbook(self, token_id: str) -> OrderBook:
        """
        Get the order book for a token.
        
        Args:
            token_id: The token/asset ID
            
        Returns:
            OrderBook with best bid/ask prices
        """
        if not self._client:
            raise RuntimeError("Client not initialized")
        
        try:
            book = self._client.get_order_book(token_id)
            
            # Extract best bid (highest buy)
            best_bid = None
            bid_size = 0.0
            if book.bids:
                best_bid = float(book.bids[0].price)
                bid_size = float(book.bids[0].size)
            
            # Extract best ask (lowest sell)
            best_ask = None
            ask_size = 0.0
            if book.asks:
                best_ask = float(book.asks[0].price)
                ask_size = float(book.asks[0].size)
            
            return OrderBook(
                token_id=token_id,
                best_bid=best_bid,
                best_ask=best_ask,
                bid_size=bid_size,
                ask_size=ask_size,
            )
            
        except Exception as e:
            logger.error(f"Failed to get orderbook for {token_id}: {e}")
            raise
    
    async def place_order(
        self,
        token_id: str,
        side: str,
        price: float,
        size: float,
    ) -> OrderResult:
        """
        Place a FAK (Fill-And-Kill) market order.
        
        FAK fills as much as immediately available and cancels the rest.
        Unlike FOK (all-or-nothing), FAK gets partial fills in thin markets,
        which is critical for hourly Polymarket markets with limited liquidity.
        
        Args:
            token_id: The token/asset ID
            side: "BUY" or "SELL"
            price: Price per share (0.01 to 0.99)
            size: For BUY: USD amount to spend. For SELL: number of shares.
            
        Returns:
            OrderResult with filled_size (shares), filled_price. On partial fill,
            filled_size < requested size. success=True if any fill occurred.
        """
        if not self._client:
            raise RuntimeError("Client not initialized")
        
        order_side = BUY if side.upper() == "BUY" else SELL
        amount = round(size, 2)
        price = round(price, 2)
        
        logger.info(f"Placing {side} FAK order: amount={amount} @ ${price} for token {token_id}")
        
        try:
            order_args = MarketOrderArgs(
                token_id=token_id,
                price=price,
                amount=amount,
                side=order_side,
                order_type=OrderType.FAK,
            )
            
            signed_order = self._client.create_market_order(order_args)
            response = self._client.post_order(signed_order, OrderType.FAK)
            
            order_id = response.get("orderID", response.get("orderId", ""))
            success = response.get("success", False) or bool(order_id)
            error_msg = response.get("errorMsg", response.get("error"))
            
            if not success and error_msg:
                logger.error(f"Failed to place order: {error_msg}")
                return OrderResult(
                    order_id="",
                    success=False,
                    filled_size=0,
                    filled_price=0,
                    error=error_msg,
                )
            
            # Fetch actual fill from order (FAK can partial fill)
            # Retry up to 3 times with backoff, then conservatively assume 0 fill
            filled_shares = 0.0
            if order_id:
                for fill_attempt in range(3):
                    try:
                        order_info = self._client.get_order(order_id)
                        if order_info and isinstance(order_info, dict):
                            order_data = order_info.get("order", order_info)
                            size_matched = order_data.get("size_matched", order_data.get("sizeMatched", "0"))
                            filled_shares = float(size_matched) if size_matched else 0.0
                        elif hasattr(order_info, "size_matched"):
                            filled_shares = float(order_info.size_matched) if order_info.size_matched else 0.0
                        break  # Success, exit retry loop
                    except Exception as e:
                        logger.warning(f"Fill check attempt {fill_attempt + 1}/3 failed for {order_id}: {e}")
                        if fill_attempt < 2:
                            await asyncio.sleep(1.0 * (fill_attempt + 1))
                        else:
                            logger.error(f"Could not verify fill for {order_id} after 3 attempts, assuming 0 fill")
                            filled_shares = 0.0
            
            if filled_shares > 0:
                logger.info(f"Order filled: {filled_shares:.2f} shares @ ${price}")
            else:
                logger.warning(f"Order placed but no fill: {order_id}")
            
            return OrderResult(
                order_id=order_id,
                success=filled_shares > 0,
                filled_size=filled_shares,
                filled_price=price if filled_shares > 0 else 0,
                error=error_msg if filled_shares == 0 else None,
            )
            
        except Exception as e:
            logger.error(f"Failed to place order: {e}")
            return OrderResult(
                order_id="",
                success=False,
                filled_size=0,
                filled_price=0,
                error=str(e),
            )
    
    async def get_open_orders(self) -> list[dict]:
        """Get all open orders for this wallet."""
        if not self._client:
            raise RuntimeError("Client not initialized")
        
        try:
            return self._client.get_orders()
        except Exception as e:
            logger.error(f"Failed to get open orders: {e}")
            return []
    
    async def cancel_order(self, order_id: str) -> bool:
        """Cancel an open order."""
        if not self._client:
            raise RuntimeError("Client not initialized")
        
        try:
            self._client.cancel(order_id)
            return True
        except Exception as e:
            logger.error(f"Failed to cancel order {order_id}: {e}")
            return False
    
    async def close(self) -> None:
        """Clean up resources."""
        self._client = None
        self._initialized = False
