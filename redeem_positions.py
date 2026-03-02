#!/usr/bin/env python3
"""Redeem resolved Polymarket positions to get USDC.e back."""

import asyncio
import time
from dotenv import load_dotenv
load_dotenv()

from web3 import Web3
from eth_account import Account
from api.data_client import DataClient
import config

# Use a more reliable RPC with higher rate limits
POLYGON_RPCS = [
    'https://polygon-rpc.com',
    'https://rpc-mainnet.matic.quiknode.pro',
    'https://polygon-mainnet.public.blastapi.io',
    'https://polygon.llamarpc.com',
]

def get_web3():
    """Get a working Web3 instance, trying multiple RPCs."""
    for rpc in POLYGON_RPCS:
        try:
            w3 = Web3(Web3.HTTPProvider(rpc, request_kwargs={'timeout': 30}))
            if w3.is_connected():
                return w3
        except:
            continue
    # Fallback to first RPC even if check fails
    return Web3(Web3.HTTPProvider(POLYGON_RPCS[0], request_kwargs={'timeout': 30}))

w3 = get_web3()

# Contract addresses
USDC_E = Web3.to_checksum_address('0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174')
CONDITIONAL_TOKENS = Web3.to_checksum_address('0x4D97DCd97eC945f40cF65F87097ACe5EA0476045')
NEG_RISK_ADAPTER = Web3.to_checksum_address('0xd91E80cF2E7be2e162c6513ceD06f1dD0dA35296')

# CTF ABI for redeemPositions
CTF_ABI = [
    {
        "inputs": [
            {"name": "collateralToken", "type": "address"},
            {"name": "parentCollectionId", "type": "bytes32"},
            {"name": "conditionId", "type": "bytes32"},
            {"name": "indexSets", "type": "uint256[]"}
        ],
        "name": "redeemPositions",
        "outputs": [],
        "stateMutability": "nonpayable",
        "type": "function"
    }
]

# NegRisk Adapter ABI for redeeming neg risk markets
NEG_RISK_ABI = [
    {
        "inputs": [
            {"name": "conditionId", "type": "bytes32"},
            {"name": "amounts", "type": "uint256[]"}
        ],
        "name": "redeemPositions",
        "outputs": [],
        "stateMutability": "nonpayable",
        "type": "function"
    }
]

# NegRisk CTF Exchange - This is the actual contract for crypto hourly markets
NEG_RISK_CTF_EXCHANGE = Web3.to_checksum_address('0xC5d563A36AE78145C45a50134d48A1215220f80a')

# Alternative ABI for the exchange adapter
NEG_RISK_EXCHANGE_ABI = [
    {
        "inputs": [
            {"name": "conditionId", "type": "bytes32"},
            {"name": "indexSets", "type": "uint256[]"}
        ],
        "name": "redeemPositions",
        "outputs": [],
        "stateMutability": "nonpayable",
        "type": "function"
    }
]

# Minimum value to attempt redemption (skip dust)
MIN_REDEMPTION_VALUE = 0.01  # $0.01


def wait_with_backoff(attempt: int, base_delay: float = 2.0, max_delay: float = 30.0):
    """Wait with exponential backoff."""
    delay = min(base_delay * (2 ** attempt), max_delay)
    print(f"  Waiting {delay:.1f}s before retry...")
    time.sleep(delay)


async def main():
    account = Account.from_key(config.PRIVATE_KEY)
    address = account.address
    
    print("=" * 70)
    print("  POLYMARKET POSITION REDEMPTION")
    print("=" * 70)
    print(f"Wallet: {address}")
    
    # Get USDC.e balance before (with retry)
    usdc_abi = [{'constant':True,'inputs':[{'name':'_owner','type':'address'}],'name':'balanceOf','outputs':[{'name':'balance','type':'uint256'}],'type':'function'}]
    usdc = w3.eth.contract(address=USDC_E, abi=usdc_abi)
    
    balance_before = None
    for attempt in range(3):
        try:
            balance_before = usdc.functions.balanceOf(address).call() / 1e6
            break
        except Exception as e:
            if attempt < 2:
                wait_with_backoff(attempt, base_delay=1.0)
            else:
                print(f"Failed to get balance: {e}")
                return
    
    print(f"USDC.e before: ${balance_before:.2f}")
    
    # Get redeemable positions
    data_client = DataClient()
    positions = await data_client.get_positions(address)
    await data_client.close()
    
    redeemable = [p for p in positions if p.redeemable]
    
    if not redeemable:
        print("\nNo redeemable positions found.")
        return
    
    print(f"\nFound {len(redeemable)} redeemable positions:")
    
    # Group by condition_id (each market has multiple outcomes sharing condition_id)
    condition_ids = {}
    for p in redeemable:
        if p.condition_id not in condition_ids:
            condition_ids[p.condition_id] = {
                'title': p.title,
                'positions': [],
                'total_value': 0
            }
        condition_ids[p.condition_id]['positions'].append(p)
        condition_ids[p.condition_id]['total_value'] += p.current_value
    
    # Sort by value (highest first) and filter out zero-value redemptions
    sorted_conditions = sorted(
        [(cid, data) for cid, data in condition_ids.items()],
        key=lambda x: x[1]['total_value'],
        reverse=True
    )
    
    # Only show and process conditions with value > MIN_REDEMPTION_VALUE
    valuable_conditions = [(cid, data) for cid, data in sorted_conditions if data['total_value'] >= MIN_REDEMPTION_VALUE]
    zero_value_count = len(sorted_conditions) - len(valuable_conditions)
    
    total_redeemable = sum(data['total_value'] for _, data in valuable_conditions)
    print(f"\nPositions with value to redeem: {len(valuable_conditions)} (${total_redeemable:.2f})")
    if zero_value_count > 0:
        print(f"Skipping {zero_value_count} positions with $0 value (lost bets)")
    
    for cid, data in valuable_conditions:
        print(f"\n  Market: {data['title'][:50]}")
        print(f"  Condition ID: {cid}")
        print(f"  Redeemable value: ${data['total_value']:.2f}")
        for p in data['positions']:
            if p.current_value >= MIN_REDEMPTION_VALUE:
                print(f"    - {p.outcome}: {p.size:.2f} shares (${p.current_value:.2f})")
    
    if not valuable_conditions:
        print("\nNo positions with value to redeem (all $0 - lost bets).")
        return
    
    print("\n" + "-" * 70)
    print(f"Redeeming {len(valuable_conditions)} positions...")
    print("(Adding delays between transactions to avoid rate limits)")
    
    ctf = w3.eth.contract(address=CONDITIONAL_TOKENS, abi=CTF_ABI)
    
    # Get fresh nonce
    nonce = None
    for attempt in range(3):
        try:
            nonce = w3.eth.get_transaction_count(address)
            break
        except Exception as e:
            if attempt < 2:
                wait_with_backoff(attempt, base_delay=1.0)
            else:
                print(f"Failed to get nonce: {e}")
                return
    
    success_count = 0
    total_recovered = 0.0
    failed_markets = []  # Track markets that fail standard redemption
    
    for idx, (cid, data) in enumerate(valuable_conditions):
        print(f"\n[{idx + 1}/{len(valuable_conditions)}] Redeeming: {data['title'][:40]}...")
        print(f"  Expected value: ${data['total_value']:.2f}")
        
        # Add delay between transactions (except first one)
        if idx > 0:
            print(f"  Waiting 3s before transaction...")
            time.sleep(3)
        
        # Always get fresh nonce before each transaction to handle network delays
        for nonce_attempt in range(3):
            try:
                nonce = w3.eth.get_transaction_count(address)
                break
            except Exception as e:
                if nonce_attempt < 2:
                    time.sleep(2)
                else:
                    print(f"  Failed to get nonce, skipping...")
                    continue
        
        max_retries = 3
        tx_sent = False
        tx_hash = None
        redeemed = False
        
        for attempt in range(max_retries):
            try:
                # Get fresh gas price for each attempt
                gas_price = int(w3.eth.gas_price * 1.3)
                
                # Only build and send if we haven't sent yet
                if not tx_sent:
                    # Standard redemption with index sets [1, 2] for binary markets
                    tx = ctf.functions.redeemPositions(
                        USDC_E,
                        bytes(32),  # parentCollectionId = 0
                        bytes.fromhex(cid[2:]),  # conditionId without 0x prefix
                        [1, 2]  # index sets for binary outcomes
                    ).build_transaction({
                        'from': address,
                        'nonce': nonce,
                        'gas': 200000,
                        'gasPrice': gas_price,
                        'chainId': 137,
                    })
                    
                    signed = account.sign_transaction(tx)
                    tx_hash = w3.eth.send_raw_transaction(signed.raw_transaction)
                    tx_sent = True
                    print(f"  Tx sent: https://polygonscan.com/tx/{tx_hash.hex()}")
                
                # Wait for confirmation with timeout
                receipt = w3.eth.wait_for_transaction_receipt(tx_hash, timeout=90)
                if receipt['status'] == 1:
                    print(f"  ✓ Redeemed successfully!")
                    success_count += 1
                    total_recovered += data['total_value']
                    redeemed = True
                    break  # Success, exit retry loop
                else:
                    print(f"  ✗ Standard redemption failed, may be neg risk market")
                    failed_markets.append((cid, data))
                    break  # Don't retry, try neg risk later
                    
            except Exception as e:
                error_msg = str(e)
                
                # Handle specific error types
                if "execution reverted" in error_msg.lower():
                    print(f"  Standard redemption reverted - trying neg risk method later")
                    failed_markets.append((cid, data))
                    break  # Don't retry reverts
                elif "rate limit" in error_msg.lower() or "-32090" in error_msg:
                    if attempt < max_retries - 1:
                        print(f"  Rate limited, retrying receipt check...")
                        wait_with_backoff(attempt, base_delay=5.0)
                    else:
                        # If tx was sent but we couldn't get receipt, it might have succeeded
                        if tx_sent:
                            print(f"  Tx may have succeeded, check: {tx_hash.hex()}")
                        else:
                            print(f"  Failed after {max_retries} attempts (rate limit)")
                elif "nonce too low" in error_msg.lower():
                    # Transaction with this nonce already exists - likely our tx succeeded
                    print(f"  Transaction likely already confirmed (nonce used)")
                    if tx_sent:
                        # Try to get receipt one more time
                        try:
                            time.sleep(3)
                            receipt = w3.eth.wait_for_transaction_receipt(tx_hash, timeout=30)
                            if receipt['status'] == 1:
                                print(f"  ✓ Confirmed: Redeemed successfully!")
                                success_count += 1
                                total_recovered += data['total_value']
                                redeemed = True
                        except:
                            print(f"  Could not confirm, check tx: {tx_hash.hex()}")
                    break
                elif "replacement transaction underpriced" in error_msg.lower():
                    # Tx already pending - just wait for it
                    print(f"  Transaction pending, waiting...")
                    if tx_sent and tx_hash:
                        try:
                            time.sleep(5)
                            receipt = w3.eth.wait_for_transaction_receipt(tx_hash, timeout=60)
                            if receipt['status'] == 1:
                                print(f"  ✓ Redeemed successfully!")
                                success_count += 1
                                total_recovered += data['total_value']
                                redeemed = True
                        except:
                            pass
                    break
                else:
                    if attempt < max_retries - 1:
                        print(f"  Error: {error_msg[:60]}... Retrying...")
                        wait_with_backoff(attempt, base_delay=3.0)
                    else:
                        print(f"  Error: {error_msg[:80]}")
    
    # Try NegRiskAdapter for failed markets
    if failed_markets:
        print(f"\n" + "=" * 70)
        print(f"  Trying NegRiskAdapter for {len(failed_markets)} failed markets...")
        print("=" * 70)
        
        neg_risk = w3.eth.contract(address=NEG_RISK_ADAPTER, abi=NEG_RISK_ABI)
        
        for idx, (cid, data) in enumerate(failed_markets):
            print(f"\n[{idx + 1}/{len(failed_markets)}] Retrying with NegRisk: {data['title'][:35]}...")
            print(f"  Expected value: ${data['total_value']:.2f}")
            
            time.sleep(3)
            
            # Get fresh nonce
            for nonce_attempt in range(3):
                try:
                    nonce = w3.eth.get_transaction_count(address)
                    break
                except:
                    time.sleep(2)
            
            try:
                gas_price = int(w3.eth.gas_price * 1.3)
                
                # Calculate amounts based on shares held (convert to wei - 18 decimals for CTF tokens)
                # The amounts array should match the index sets
                positions = data['positions']
                amounts = []
                for p in positions:
                    # Convert to base units (shares are already in decimals, need to convert to wei)
                    amount_wei = int(p.size * 10**6)  # USDC has 6 decimals
                    amounts.append(amount_wei)
                
                # Pad amounts array if needed (binary markets have 2 outcomes)
                while len(amounts) < 2:
                    amounts.append(0)
                
                tx = neg_risk.functions.redeemPositions(
                    bytes.fromhex(cid[2:]),  # conditionId
                    amounts,
                ).build_transaction({
                    'from': address,
                    'nonce': nonce,
                    'gas': 300000,
                    'gasPrice': gas_price,
                    'chainId': 137,
                })
                
                signed = account.sign_transaction(tx)
                tx_hash = w3.eth.send_raw_transaction(signed.raw_transaction)
                print(f"  Tx sent: https://polygonscan.com/tx/{tx_hash.hex()}")
                
                receipt = w3.eth.wait_for_transaction_receipt(tx_hash, timeout=90)
                if receipt['status'] == 1:
                    print(f"  ✓ Redeemed via NegRisk!")
                    success_count += 1
                    total_recovered += data['total_value']
                else:
                    print(f"  ✗ NegRisk redemption also failed")
                    print(f"    This position may require manual redemption via Polymarket UI")
                    
            except Exception as e:
                error_msg = str(e)
                if "rate limit" in error_msg.lower():
                    print(f"  Rate limited - try again later")
                else:
                    print(f"  NegRisk error: {error_msg[:60]}")
                    print(f"    Try redeeming manually at: https://polymarket.com/portfolio")
    
    # Wait a bit then get final balance
    print("\nWaiting for transactions to settle...")
    time.sleep(5)
    
    # Get USDC.e balance after (with retry)
    balance_after = None
    for attempt in range(3):
        try:
            balance_after = usdc.functions.balanceOf(address).call() / 1e6
            break
        except Exception as e:
            if attempt < 2:
                wait_with_backoff(attempt, base_delay=2.0)
            else:
                print(f"Failed to get final balance: {e}")
                balance_after = balance_before  # Fallback
    
    print("\n" + "=" * 70)
    print("  REDEMPTION COMPLETE")
    print("=" * 70)
    print(f"USDC.e before: ${balance_before:.2f}")
    print(f"USDC.e after:  ${balance_after:.2f}")
    print(f"Recovered:     ${balance_after - balance_before:.2f}")
    print(f"Expected:      ${total_recovered:.2f}")
    print(f"Markets redeemed: {success_count}/{len(valuable_conditions)}")


if __name__ == "__main__":
    asyncio.run(main())
