"""Test trade - check target's last trade and copy it (max $10)."""

import asyncio
from dotenv import load_dotenv
load_dotenv()

from api.data_client import DataClient
from api.clob_client import ClobClient
from eth_account import Account
import config


async def get_target_positions():
    """Get active positions from target account."""
    # Use the specific wallet address
    target_wallet = "0xd8f8c13644ea84d62e1ec88c5d1215e436eb0f11"
    print(f"Fetching data for wallet: {target_wallet}")
    
    data_client = DataClient()
    try:
        # Get profile info (optional, for display)
        profile = await data_client.get_profile_by_address(target_wallet)
        if profile:
            print(f"Profile: {profile.username or profile.name or 'N/A'}")
        
        # Get active positions
        positions = await data_client.get_positions(wallet=target_wallet)
        if positions:
            print(f"\n--- Active Positions ({len(positions)}) ---")
            for pos in positions[:10]:
                print(f"  - {pos.size:.4f} shares @ avg ${pos.avg_price:.4f}")
                print(f"    {pos.title[:60]} - {pos.outcome}")
                print(f"    Token: {pos.token_id}")
        else:
            print("\nNo active positions")
        
        return positions
        
    except Exception as e:
        print(f"Error fetching target positions: {e}")
        import traceback
        traceback.print_exc()
        return None
    finally:
        await data_client.close()


async def execute_copy_position(positions, max_usd=10.0):
    """Buy into the target's latest active position."""
    print("\n" + "=" * 50)
    print("Initializing CLOB client...")
    
    account = Account.from_key(config.PRIVATE_KEY)
    print(f"Wallet: {account.address}")
    
    clob_client = ClobClient()
    try:
        await clob_client.initialize()
        print("✓ CLOB client initialized")
        
        if not positions:
            print("No positions to copy")
            return
        
        # Find the first position with an active orderbook
        position = None
        current_price = None
        
        for pos in positions:
            try:
                orderbook = await clob_client.get_orderbook(pos.token_id)
                if orderbook.best_ask:
                    position = pos
                    current_price = orderbook.best_ask
                    break
                else:
                    print(f"  Skipping (no liquidity): {pos.title[:50]}")
            except Exception as e:
                print(f"  Skipping (market closed): {pos.title[:50]}")
                continue
        
        if not position or not current_price:
            print("No active positions with orderbooks found")
            return
        
        # Calculate size based on max USD
        size = max_usd / current_price
        
        print(f"\nBuying into active position:")
        print(f"  Market: {position.title[:60]} - {position.outcome}")
        print(f"  Token: {position.token_id}")
        print(f"  Target's position: {position.size:.4f} shares @ avg ${position.avg_price:.4f}")
        print(f"  Current ask price: ${current_price:.4f}")
        print(f"  Our size: {size:.4f} shares")
        print(f"  Our total: ${current_price * size:.2f}")
        
        # Place the BUY order
        result = await clob_client.place_order(
            token_id=position.token_id,
            side="BUY",
            price=current_price,
            size=size,
        )
        
        if result.success:
            print(f"\n✓ Order placed successfully!")
            print(f"  Order ID: {result.order_id}")
        else:
            print(f"\n✗ Order failed: {result.error}")
        
    except Exception as e:
        print(f"✗ Error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        await clob_client.close()


async def main():
    print("=" * 50)
    print("COPY POSITION TEST (max $10)")
    print("=" * 50)
    
    # Get target's active positions
    positions = await get_target_positions()
    
    # Buy into their top position
    await execute_copy_position(positions, max_usd=10.0)


if __name__ == "__main__":
    asyncio.run(main())