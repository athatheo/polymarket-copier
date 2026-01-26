#!/usr/bin/env python3
"""
Daily status check script.
Shows: wallet balance, active positions, and recent trade history.
"""

import asyncio
from datetime import datetime, timedelta
from dotenv import load_dotenv
load_dotenv()

from web3 import Web3
from eth_account import Account
from api.data_client import DataClient
from storage.state import StateStorage
import config


async def main():
    account = Account.from_key(config.PRIVATE_KEY)
    address = account.address
    
    print("=" * 70)
    print(f"  POLYMARKET COPY BOT - DAILY STATUS")
    print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 70)
    
    # =========================================================================
    # WALLET BALANCE
    # =========================================================================
    print("\n📊 WALLET BALANCE")
    print("-" * 70)
    print(f"Address: {address}")
    
    w3 = Web3(Web3.HTTPProvider('https://polygon-rpc.com', request_kwargs={'timeout': 30}))
    
    # MATIC/POL
    matic = w3.eth.get_balance(address)
    matic_formatted = float(w3.from_wei(matic, 'ether'))
    print(f"POL/MATIC:  {matic_formatted:.4f}")
    
    # USDC.e
    USDC = '0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174'
    ABI = [{'constant':True,'inputs':[{'name':'_owner','type':'address'}],'name':'balanceOf','outputs':[{'name':'balance','type':'uint256'}],'type':'function'}]
    usdc = w3.eth.contract(address=Web3.to_checksum_address(USDC), abi=ABI)
    usdc_bal = usdc.functions.balanceOf(address).call() / 1e6
    print(f"USDC.e:     ${usdc_bal:.2f}")
    
    # =========================================================================
    # ACTIVE POSITIONS
    # =========================================================================
    print("\n📈 ACTIVE POSITIONS")
    print("-" * 70)
    
    data_client = DataClient()
    try:
        positions = await data_client.get_positions(address)
        
        if positions:
            total_value = 0
            print(f"{'Market':<45} {'Outcome':<10} {'Shares':>10} {'Value':>10}")
            print("-" * 70)
            
            for pos in positions:
                title = pos.title[:42] + "..." if len(pos.title) > 45 else pos.title
                outcome = pos.outcome[:8] if len(pos.outcome) > 10 else pos.outcome
                value = pos.current_value
                total_value += value
                print(f"{title:<45} {outcome:<10} {pos.size:>10.2f} ${value:>9.2f}")
            
            print("-" * 70)
            print(f"{'TOTAL POSITION VALUE':<67} ${total_value:>9.2f}")
        else:
            print("No active positions")
        
        # =========================================================================
        # TOTAL PORTFOLIO VALUE
        # =========================================================================
        print("\n💰 TOTAL PORTFOLIO")
        print("-" * 70)
        total_portfolio = usdc_bal + (total_value if positions else 0)
        print(f"USDC.e (cash):     ${usdc_bal:.2f}")
        print(f"Positions:         ${total_value if positions else 0:.2f}")
        print(f"TOTAL:             ${total_portfolio:.2f}")
        
    finally:
        await data_client.close()
    
    # =========================================================================
    # RECENT COPIED TRADES
    # =========================================================================
    print("\n📋 RECENT COPIED TRADES (Last 7 days)")
    print("-" * 70)
    
    state = StateStorage()
    await state.initialize()
    
    try:
        recent_trades = await state.get_recent_trades(limit=50)
        
        # Filter to last 7 days
        week_ago = datetime.utcnow() - timedelta(days=7)
        recent_trades = [t for t in recent_trades if t.created_at >= week_ago]
        
        if recent_trades:
            print(f"{'Date':<12} {'Status':<15} {'Side':<6} {'Size':>8} {'Price':>8}")
            print("-" * 70)
            
            for trade in recent_trades[:20]:
                date = trade.created_at.strftime('%m/%d %H:%M')
                status = trade.status.value[:13]
                side = trade.side[:4]
                size = f"{trade.my_size:.2f}" if trade.my_size else "-"
                price = f"${trade.my_price:.4f}" if trade.my_price else "-"
                print(f"{date:<12} {status:<15} {side:<6} {size:>8} {price:>8}")
            
            if len(recent_trades) > 20:
                print(f"  ... and {len(recent_trades) - 20} more")
        else:
            print("No trades in the last 7 days")
        
        # Stats
        stats = await state.get_stats()
        print("\n📊 ALL-TIME STATS")
        print("-" * 70)
        print(f"Copied:            {stats.get('copied', 0)}")
        print(f"Skipped (slippage): {stats.get('skipped_slippage', 0)}")
        print(f"Skipped (size):     {stats.get('skipped_size', 0)}")
        print(f"Skipped (no pos):   {stats.get('skipped_no_position', 0)}")
        print(f"Skipped (error):    {stats.get('skipped_error', 0)}")
        
    finally:
        await state.close()
    
    # =========================================================================
    # TARGET INFO
    # =========================================================================
    print("\n🎯 TARGET ACCOUNT")
    print("-" * 70)
    target_wallet = config.TARGET_WALLET or "Not set"
    print(f"Wallet: {target_wallet}")
    
    if config.TARGET_WALLET:
        data_client = DataClient()
        try:
            target_positions = await data_client.get_positions(config.TARGET_WALLET)
            print(f"Active positions: {len(target_positions) if target_positions else 0}")
        finally:
            await data_client.close()
    
    print("\n" + "=" * 70)
    print("  Status check complete")
    print("=" * 70)


if __name__ == "__main__":
    asyncio.run(main())
