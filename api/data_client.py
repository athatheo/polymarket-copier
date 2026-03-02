"""
Read-only client for Polymarket Data and Gamma APIs.

Handles:
- Profile lookup by username
- Trade history fetching
- Position fetching
- Portfolio value calculation
"""

import httpx
from dataclasses import dataclass
from typing import Optional
import logging

import config

logger = logging.getLogger(__name__)


@dataclass
class Trade:
    """Represents a single trade from the target account."""
    tx_hash: str
    token_id: str  # The asset/token being traded
    condition_id: str
    side: str  # "BUY" or "SELL"
    size: float  # Number of shares
    price: float  # Price per share
    timestamp: int  # Unix timestamp
    title: str  # Market title
    outcome: str  # Outcome name (e.g., "Yes", "No")


@dataclass
class Position:
    """Represents a current position."""
    token_id: str
    condition_id: str
    size: float
    avg_price: float
    current_value: float
    title: str
    outcome: str
    cur_price: float = 0.0
    redeemable: bool = False
    end_date: str = ""


@dataclass
class Profile:
    """Represents a Polymarket user profile."""
    wallet_address: str
    username: Optional[str]
    name: Optional[str]


class DataClient:
    """
    Client for Polymarket's read-only APIs.
    
    Uses:
    - Gamma API for profile search
    - Data API for trades and positions
    """
    
    def __init__(self):
        self._http = httpx.AsyncClient(timeout=30.0)
    
    async def close(self):
        """Close the HTTP client."""
        await self._http.aclose()
    
    async def search_profile(self, username: str) -> Optional[Profile]:
        """
        Search for a user profile by username.
        
        Args:
            username: The Polymarket username to search for
            
        Returns:
            Profile with wallet address, or None if not found
        """
        url = f"{config.GAMMA_API_URL}/public-search"
        params = {
            "q": username,
            "search_profiles": "true",
            "limit_per_type": 10,
        }
        
        try:
            response = await self._http.get(url, params=params)
            response.raise_for_status()
            data = response.json()
            
            profiles = data.get("profiles", [])
            if not profiles:
                logger.warning(f"No profile found for username: {username}")
                return None
            
            # Find exact match or closest match
            for profile in profiles:
                pseudonym = profile.get("pseudonym", "")
                name = profile.get("name", "")
                
                if pseudonym.lower() == username.lower() or name.lower() == username.lower():
                    wallet = profile.get("proxyWallet")
                    if wallet:
                        return Profile(
                            wallet_address=wallet,
                            username=pseudonym,
                            name=name,
                        )
            
            # Fall back to first result if no exact match
            first = profiles[0]
            wallet = first.get("proxyWallet")
            if wallet:
                return Profile(
                    wallet_address=wallet,
                    username=first.get("pseudonym"),
                    name=first.get("name"),
                )
            
            return None
            
        except httpx.HTTPError as e:
            logger.error(f"Failed to search profile: {e}")
            raise
    
    async def get_trades(
        self, 
        wallet: str, 
        since_timestamp: Optional[int] = None,
        limit: int = 100,
    ) -> list[Trade]:
        """
        Get recent trades for a wallet address.
        
        Uses the /activity endpoint which returns complete trade data,
        unlike /trades which can miss recent transactions.
        
        Args:
            wallet: The wallet address (proxy wallet)
            since_timestamp: Only return trades after this timestamp
            limit: Maximum number of trades to return
            
        Returns:
            List of Trade objects, newest first
        """
        # Use /activity endpoint - /trades endpoint misses recent trades
        url = f"{config.DATA_API_URL}/activity"
        params = {
            "user": wallet,
            "limit": limit,
        }
        
        try:
            response = await self._http.get(url, params=params)
            response.raise_for_status()
            data = response.json()
            
            trades = []
            for item in data:
                # Filter for TRADE type only (activity includes other types)
                if item.get("type") != "TRADE":
                    continue
                
                timestamp = item.get("timestamp", 0)
                
                # Filter by timestamp if specified
                if since_timestamp and timestamp <= since_timestamp:
                    continue
                
                trade = Trade(
                    tx_hash=item.get("transactionHash", ""),
                    token_id=item.get("asset", ""),
                    condition_id=item.get("conditionId", ""),
                    side=item.get("side", ""),
                    size=float(item.get("size", 0)),
                    price=float(item.get("price", 0)),
                    timestamp=timestamp,
                    title=item.get("title", ""),
                    outcome=item.get("outcome", ""),
                )
                trades.append(trade)
            
            return trades
            
        except httpx.HTTPError as e:
            logger.error(f"Failed to get trades: {e}")
            raise
    
    async def get_positions(self, wallet: str) -> list[Position]:
        """
        Get current positions for a wallet.
        
        Args:
            wallet: The wallet address (proxy wallet)
            
        Returns:
            List of Position objects
        """
        url = f"{config.DATA_API_URL}/positions"
        params = {
            "user": wallet,
            "limit": 500,
            "sizeThreshold": 0.01,  # Ignore dust
        }
        
        try:
            response = await self._http.get(url, params=params)
            response.raise_for_status()
            data = response.json()
            
            positions = []
            for item in data:
                position = Position(
                    token_id=item.get("asset", ""),
                    condition_id=item.get("conditionId", ""),
                    size=float(item.get("size", 0)),
                    avg_price=float(item.get("avgPrice", 0)),
                    current_value=float(item.get("currentValue", 0)),
                    title=item.get("title", ""),
                    outcome=item.get("outcome", ""),
                    cur_price=float(item.get("curPrice", 0)),
                    redeemable=item.get("redeemable", False),
                    end_date=item.get("endDate", ""),
                )
                positions.append(position)
            
            return positions
            
        except httpx.HTTPError as e:
            logger.error(f"Failed to get positions: {e}")
            raise
    
    async def get_portfolio_value(self, wallet: str) -> float:
        """
        Calculate total portfolio value for a wallet.
        
        Args:
            wallet: The wallet address (proxy wallet)
            
        Returns:
            Total value in USD
        """
        url = f"{config.DATA_API_URL}/value"
        params = {"user": wallet}
        
        try:
            response = await self._http.get(url, params=params)
            response.raise_for_status()
            data = response.json()
            
            # The API returns total value directly
            return float(data.get("value", 0))
            
        except httpx.HTTPError as e:
            # Fall back to summing positions if /value endpoint fails
            logger.warning(f"Value endpoint failed, falling back to position sum: {e}")
            positions = await self.get_positions(wallet)
            return sum(p.current_value for p in positions)
    
    async def get_profile_by_address(self, wallet: str) -> Optional[Profile]:
        """
        Get profile info by wallet address.
        
        Args:
            wallet: The wallet address
            
        Returns:
            Profile info, or None if not found
        """
        url = f"{config.GAMMA_API_URL}/public-profile"
        params = {"address": wallet}
        
        try:
            response = await self._http.get(url, params=params)
            response.raise_for_status()
            data = response.json()
            
            return Profile(
                wallet_address=data.get("proxyWallet", wallet),
                username=data.get("pseudonym"),
                name=data.get("name"),
            )
            
        except httpx.HTTPError as e:
            logger.error(f"Failed to get profile: {e}")
            return None
