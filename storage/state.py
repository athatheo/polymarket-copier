"""
SQLite storage for tracking copied trades.

Prevents duplicate copies and provides trade history.
"""

import aiosqlite
import json
import logging
import os
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Optional
from enum import Enum

import config

# Portfolio history file path
PORTFOLIO_HISTORY_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)), "portfolio_history.json")

logger = logging.getLogger(__name__)


class TradeStatus(Enum):
    """Status of a copied trade."""
    COPIED = "copied"
    SKIPPED_SLIPPAGE = "skipped_slippage"
    SKIPPED_SIZE = "skipped_size"
    SKIPPED_ERROR = "skipped_error"
    SKIPPED_NO_POSITION = "skipped_no_position"  # Can't SELL without owning
    PENDING = "pending"


@dataclass
class CopiedTrade:
    """Record of a trade copy attempt."""
    id: Optional[int]
    target_tx_hash: str
    token_id: str
    condition_id: str
    side: str
    target_price: float
    target_size: float
    my_price: Optional[float]
    my_size: Optional[float]
    my_order_id: Optional[str]
    status: TradeStatus
    skip_reason: Optional[str]
    created_at: datetime


class StateStorage:
    """
    SQLite storage for trade state.
    
    Tracks which trades have been copied to prevent duplicates.
    """
    
    def __init__(self, db_path: Optional[str] = None):
        self._db_path = db_path or config.DATABASE_PATH
        self._db: Optional[aiosqlite.Connection] = None
    
    async def initialize(self) -> None:
        """Initialize the database and create tables."""
        self._db = await aiosqlite.connect(self._db_path)
        
        # Enable foreign keys and WAL mode for better performance
        await self._db.execute("PRAGMA foreign_keys = ON")
        await self._db.execute("PRAGMA journal_mode = WAL")
        
        # Create the copied_trades table
        await self._db.execute("""
            CREATE TABLE IF NOT EXISTS copied_trades (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                target_tx_hash TEXT UNIQUE NOT NULL,
                token_id TEXT NOT NULL,
                condition_id TEXT NOT NULL,
                side TEXT NOT NULL,
                target_price REAL NOT NULL,
                target_size REAL NOT NULL,
                my_price REAL,
                my_size REAL,
                my_order_id TEXT,
                status TEXT NOT NULL,
                skip_reason TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Create index for faster lookups
        await self._db.execute("""
            CREATE INDEX IF NOT EXISTS idx_target_tx_hash 
            ON copied_trades(target_tx_hash)
        """)
        
        await self._db.execute("""
            CREATE INDEX IF NOT EXISTS idx_status 
            ON copied_trades(status)
        """)
        
        await self._db.commit()
        logger.info(f"Database initialized at {self._db_path}")
    
    async def close(self) -> None:
        """Close the database connection."""
        if self._db:
            await self._db.close()
            self._db = None
    
    async def is_already_copied(self, target_tx_hash: str) -> bool:
        """
        Check if a trade has already been processed.
        
        Args:
            target_tx_hash: The transaction hash from the target's trade
            
        Returns:
            True if this trade was already copied or skipped
        """
        if not self._db:
            raise RuntimeError("Database not initialized")
        
        cursor = await self._db.execute(
            "SELECT 1 FROM copied_trades WHERE target_tx_hash = ?",
            (target_tx_hash,)
        )
        row = await cursor.fetchone()
        return row is not None
    
    async def record_copied(
        self,
        target_tx_hash: str,
        token_id: str,
        condition_id: str,
        side: str,
        target_price: float,
        target_size: float,
        my_price: float,
        my_size: float,
        my_order_id: str,
    ) -> CopiedTrade:
        """
        Record a successfully copied trade.
        
        Args:
            target_tx_hash: Transaction hash from target's trade
            token_id: The token/asset traded
            condition_id: The market condition ID
            side: "BUY" or "SELL"
            target_price: Price the target paid
            target_size: Size the target traded
            my_price: Price we paid
            my_size: Size we traded
            my_order_id: Our order ID
            
        Returns:
            The created CopiedTrade record
        """
        if not self._db:
            raise RuntimeError("Database not initialized")
        
        cursor = await self._db.execute(
            """
            INSERT INTO copied_trades 
            (target_tx_hash, token_id, condition_id, side, target_price, target_size,
             my_price, my_size, my_order_id, status)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (target_tx_hash, token_id, condition_id, side, target_price, target_size,
             my_price, my_size, my_order_id, TradeStatus.COPIED.value)
        )
        await self._db.commit()
        
        trade = CopiedTrade(
            id=cursor.lastrowid,
            target_tx_hash=target_tx_hash,
            token_id=token_id,
            condition_id=condition_id,
            side=side,
            target_price=target_price,
            target_size=target_size,
            my_price=my_price,
            my_size=my_size,
            my_order_id=my_order_id,
            status=TradeStatus.COPIED,
            skip_reason=None,
            created_at=datetime.utcnow(),
        )
        
        logger.info(f"Recorded copied trade: {target_tx_hash}")
        return trade
    
    async def record_skipped(
        self,
        target_tx_hash: str,
        token_id: str,
        condition_id: str,
        side: str,
        target_price: float,
        target_size: float,
        status: TradeStatus,
        reason: str,
    ) -> CopiedTrade:
        """
        Record a skipped trade.
        
        Args:
            target_tx_hash: Transaction hash from target's trade
            token_id: The token/asset
            condition_id: The market condition ID
            side: "BUY" or "SELL"
            target_price: Price the target paid
            target_size: Size the target traded
            status: Why it was skipped
            reason: Human-readable reason
            
        Returns:
            The created CopiedTrade record
        """
        if not self._db:
            raise RuntimeError("Database not initialized")
        
        cursor = await self._db.execute(
            """
            INSERT INTO copied_trades 
            (target_tx_hash, token_id, condition_id, side, target_price, target_size,
             status, skip_reason)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (target_tx_hash, token_id, condition_id, side, target_price, target_size,
             status.value, reason)
        )
        await self._db.commit()
        
        trade = CopiedTrade(
            id=cursor.lastrowid,
            target_tx_hash=target_tx_hash,
            token_id=token_id,
            condition_id=condition_id,
            side=side,
            target_price=target_price,
            target_size=target_size,
            my_price=None,
            my_size=None,
            my_order_id=None,
            status=status,
            skip_reason=reason,
            created_at=datetime.utcnow(),
        )
        
        logger.warning(f"Recorded skipped trade: {target_tx_hash} - {reason}")
        return trade
    
    async def get_recent_trades(self, limit: int = 100) -> list[CopiedTrade]:
        """
        Get recent copied/skipped trades.
        
        Args:
            limit: Maximum number of trades to return
            
        Returns:
            List of CopiedTrade records, newest first
        """
        if not self._db:
            raise RuntimeError("Database not initialized")
        
        cursor = await self._db.execute(
            """
            SELECT id, target_tx_hash, token_id, condition_id, side, 
                   target_price, target_size, my_price, my_size, my_order_id,
                   status, skip_reason, created_at
            FROM copied_trades
            ORDER BY created_at DESC
            LIMIT ?
            """,
            (limit,)
        )
        
        rows = await cursor.fetchall()
        trades = []
        
        for row in rows:
            trade = CopiedTrade(
                id=row[0],
                target_tx_hash=row[1],
                token_id=row[2],
                condition_id=row[3],
                side=row[4],
                target_price=row[5],
                target_size=row[6],
                my_price=row[7],
                my_size=row[8],
                my_order_id=row[9],
                status=TradeStatus(row[10]),
                skip_reason=row[11],
                created_at=datetime.fromisoformat(row[12]) if row[12] else datetime.utcnow(),
            )
            trades.append(trade)
        
        return trades
    
    async def get_stats(self) -> dict:
        """
        Get statistics about copied trades.
        
        Returns:
            Dict with counts by status
        """
        if not self._db:
            raise RuntimeError("Database not initialized")
        
        cursor = await self._db.execute(
            """
            SELECT status, COUNT(*) as count
            FROM copied_trades
            GROUP BY status
            """
        )
        
        rows = await cursor.fetchall()
        stats = {status.value: 0 for status in TradeStatus}
        
        for row in rows:
            stats[row[0]] = row[1]
        
        return stats
    
    async def get_last_processed_timestamp(self) -> Optional[int]:
        """
        Get the timestamp of the most recently processed trade.
        
        Returns:
            Unix timestamp, or None if no trades processed
        """
        if not self._db:
            raise RuntimeError("Database not initialized")
        
        cursor = await self._db.execute(
            """
            SELECT MAX(created_at) FROM copied_trades
            """
        )
        
        row = await cursor.fetchone()
        if row and row[0]:
            dt = datetime.fromisoformat(row[0])
            return int(dt.timestamp())
        
        return None


# =============================================================================
# Portfolio History Functions (file-based for persistence)
# =============================================================================

def record_portfolio_snapshot(
    total_portfolio: float,
    usdc_balance: float,
    positions_value: float,
    matic_balance: float = 0.0,
) -> dict:
    """
    Record a portfolio snapshot to the history file.
    
    Args:
        total_portfolio: Total portfolio value in USD
        usdc_balance: USDC.e balance
        positions_value: Value of all positions
        matic_balance: POL/MATIC balance
        
    Returns:
        The recorded snapshot dict
    """
    snapshot = {
        "timestamp": datetime.utcnow().isoformat(),
        "total_portfolio": round(total_portfolio, 2),
        "usdc_balance": round(usdc_balance, 2),
        "positions_value": round(positions_value, 2),
        "matic_balance": round(matic_balance, 4),
    }
    
    # Load existing history
    history = []
    if os.path.exists(PORTFOLIO_HISTORY_PATH):
        try:
            with open(PORTFOLIO_HISTORY_PATH, "r") as f:
                history = json.load(f)
        except (json.JSONDecodeError, IOError):
            history = []
    
    # Append new snapshot
    history.append(snapshot)
    
    # Write back
    with open(PORTFOLIO_HISTORY_PATH, "w") as f:
        json.dump(history, f, indent=2)
    
    return snapshot


def get_portfolio_history(time_range: str = "24h") -> list[dict]:
    """
    Get portfolio history for a given time range.
    
    Args:
        time_range: One of "24h", "3d", "7d", "1m", "3m", "6m", "1y", "all"
        
    Returns:
        List of portfolio snapshots within the time range
    """
    if not os.path.exists(PORTFOLIO_HISTORY_PATH):
        return []
    
    try:
        with open(PORTFOLIO_HISTORY_PATH, "r") as f:
            history = json.load(f)
    except (json.JSONDecodeError, IOError):
        return []
    
    if time_range == "all":
        return history
    
    # Calculate cutoff time
    now = datetime.utcnow()
    range_mapping = {
        "24h": timedelta(hours=24),
        "3d": timedelta(days=3),
        "7d": timedelta(days=7),
        "1m": timedelta(days=30),
        "3m": timedelta(days=90),
        "6m": timedelta(days=180),
        "1y": timedelta(days=365),
    }
    
    delta = range_mapping.get(time_range, timedelta(hours=24))
    cutoff = now - delta
    
    # Filter snapshots
    filtered = []
    for snap in history:
        try:
            snap_time = datetime.fromisoformat(snap["timestamp"])
            if snap_time >= cutoff:
                filtered.append(snap)
        except (KeyError, ValueError):
            continue
    
    return filtered


def get_portfolio_stats() -> dict:
    """
    Get portfolio statistics from history.
    
    Returns:
        Dict with stats like first_value, current_value, change, etc.
    """
    history = get_portfolio_history("all")
    
    if not history:
        return {
            "total_snapshots": 0,
            "first_value": None,
            "current_value": None,
            "all_time_high": None,
            "all_time_low": None,
            "change_24h": None,
            "change_pct_24h": None,
        }
    
    current = history[-1]["total_portfolio"]
    first = history[0]["total_portfolio"]
    
    all_values = [s["total_portfolio"] for s in history]
    ath = max(all_values)
    atl = min(all_values)
    
    # 24h change
    history_24h = get_portfolio_history("24h")
    if len(history_24h) > 1:
        change_24h = current - history_24h[0]["total_portfolio"]
        change_pct_24h = (change_24h / history_24h[0]["total_portfolio"]) * 100 if history_24h[0]["total_portfolio"] > 0 else 0
    else:
        change_24h = None
        change_pct_24h = None
    
    return {
        "total_snapshots": len(history),
        "first_value": first,
        "current_value": current,
        "all_time_high": ath,
        "all_time_low": atl,
        "change_24h": round(change_24h, 2) if change_24h is not None else None,
        "change_pct_24h": round(change_pct_24h, 2) if change_pct_24h is not None else None,
    }


# =============================================================================
# Dry Run Trade Persistence (for P&L analytics dashboard)
# =============================================================================

DRY_RUN_TRADES_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)), "dry_run_trades.json")


def record_dry_run_trade(trade: dict) -> None:
    """Append a dry run trade to the history file."""
    trades = []
    if os.path.exists(DRY_RUN_TRADES_PATH):
        try:
            with open(DRY_RUN_TRADES_PATH, "r") as f:
                trades = json.load(f)
        except (json.JSONDecodeError, IOError):
            trades = []
    trades.append(trade)
    with open(DRY_RUN_TRADES_PATH, "w") as f:
        json.dump(trades, f, indent=2)


def get_dry_run_trades(since: Optional[str] = None) -> list[dict]:
    """Get dry run trades, optionally filtered by timestamp string."""
    if not os.path.exists(DRY_RUN_TRADES_PATH):
        return []
    try:
        with open(DRY_RUN_TRADES_PATH, "r") as f:
            trades = json.load(f)
    except (json.JSONDecodeError, IOError):
        return []
    if since:
        trades = [t for t in trades if t.get("timestamp", "") >= since]
    return trades
