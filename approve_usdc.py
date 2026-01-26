"""Approve USDC.e for Polymarket trading - complete setup."""

from dotenv import load_dotenv
load_dotenv()

import time
from eth_account import Account
from web3 import Web3
import config

# Polygon RPC
POLYGON_RPC = "https://polygon-rpc.com"

# Contract addresses (from Polymarket)
USDC_CONTRACT = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"  # USDC.e
CTF_EXCHANGE = "0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E"
CONDITIONAL_TOKENS = "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045"
NEG_RISK_CTF_EXCHANGE = "0xC5d563A36AE78145C45a50134d48A1215220f80a"
NEG_RISK_ADAPTER = "0xd91E80cF2E7be2e162c6513ceD06f1dD0dA35296"

# ERC20 ABI
ERC20_ABI = [
    {"constant": True, "inputs": [{"name": "_owner", "type": "address"}, {"name": "_spender", "type": "address"}], "name": "allowance", "outputs": [{"name": "", "type": "uint256"}], "type": "function"},
    {"constant": False, "inputs": [{"name": "_spender", "type": "address"}, {"name": "_value", "type": "uint256"}], "name": "approve", "outputs": [{"name": "", "type": "bool"}], "type": "function"},
    {"constant": True, "inputs": [{"name": "_owner", "type": "address"}], "name": "balanceOf", "outputs": [{"name": "balance", "type": "uint256"}], "type": "function"}
]

# ERC1155 approval ABI (for conditional tokens)
ERC1155_ABI = [
    {"constant": True, "inputs": [{"name": "_owner", "type": "address"}, {"name": "_operator", "type": "address"}], "name": "isApprovedForAll", "outputs": [{"name": "", "type": "bool"}], "type": "function"},
    {"constant": False, "inputs": [{"name": "_operator", "type": "address"}, {"name": "_approved", "type": "bool"}], "name": "setApprovalForAll", "outputs": [], "type": "function"},
]


def send_tx(w3, tx, private_key, description):
    """Send and wait for transaction."""
    signed = w3.eth.account.sign_transaction(tx, private_key)
    tx_hash = w3.eth.send_raw_transaction(signed.raw_transaction)
    print(f"  Tx sent: 0x{tx_hash.hex()}")
    
    for i in range(30):
        time.sleep(3)
        try:
            receipt = w3.eth.get_transaction_receipt(tx_hash)
            if receipt:
                if receipt.status == 1:
                    print(f"  ✓ {description} successful (gas: {receipt.gasUsed})")
                    return True
                else:
                    print(f"  ✗ {description} failed!")
                    return False
        except:
            pass
    print(f"  Timeout waiting for {description}")
    return False


def approve_usdc():
    account = Account.from_key(config.PRIVATE_KEY)
    address = account.address
    
    print(f"Wallet: {address}")
    print("=" * 60)
    
    w3 = Web3(Web3.HTTPProvider(POLYGON_RPC, request_kwargs={'timeout': 30}))
    
    try:
        chain_id = w3.eth.chain_id
        print(f"Connected to Polygon (Chain ID: {chain_id})")
    except Exception as e:
        print(f"Failed to connect: {e}")
        return
    
    usdc = w3.eth.contract(address=Web3.to_checksum_address(USDC_CONTRACT), abi=ERC20_ABI)
    ctf = w3.eth.contract(address=Web3.to_checksum_address(CONDITIONAL_TOKENS), abi=ERC1155_ABI)
    
    # Check USDC balance
    balance = usdc.functions.balanceOf(address).call()
    print(f"\nUSDC.e Balance: ${balance / 1e6:.2f}")
    
    # Check MATIC balance
    matic = w3.eth.get_balance(address)
    print(f"POL/MATIC: {w3.from_wei(matic, 'ether'):.4f}")
    
    max_approval = 2**256 - 1
    nonce = w3.eth.get_transaction_count(address)
    gas_price = int(w3.eth.gas_price * 1.3)
    
    print(f"\nGas price: {gas_price / 1e9:.2f} Gwei")
    print(f"Starting nonce: {nonce}")
    
    # List of approvals needed
    approvals = [
        ("USDC -> CTF Exchange", usdc, CTF_EXCHANGE, "allowance", "approve", max_approval),
        ("USDC -> NegRisk CTF Exchange", usdc, NEG_RISK_CTF_EXCHANGE, "allowance", "approve", max_approval),
    ]
    
    ctf_approvals = [
        ("CTF -> CTF Exchange", ctf, CTF_EXCHANGE, "isApprovedForAll", "setApprovalForAll", True),
        ("CTF -> NegRisk Adapter", ctf, NEG_RISK_ADAPTER, "isApprovedForAll", "setApprovalForAll", True),
    ]
    
    print("\n--- USDC Approvals ---")
    for name, contract, spender, check_fn, approve_fn, value in approvals:
        spender_addr = Web3.to_checksum_address(spender)
        current = getattr(contract.functions, check_fn)(address, spender_addr).call()
        
        if current > 0:
            print(f"✓ {name}: Already approved")
            continue
        
        print(f"  {name}: Approving...")
        tx = getattr(contract.functions, approve_fn)(spender_addr, value).build_transaction({
            'from': address,
            'nonce': nonce,
            'gas': 60000,
            'gasPrice': gas_price,
            'chainId': 137,
        })
        
        if send_tx(w3, tx, config.PRIVATE_KEY, name):
            nonce += 1
        else:
            print(f"  Failed to approve {name}")
    
    print("\n--- Conditional Token Approvals ---")
    for name, contract, spender, check_fn, approve_fn, value in ctf_approvals:
        spender_addr = Web3.to_checksum_address(spender)
        current = getattr(contract.functions, check_fn)(address, spender_addr).call()
        
        if current:
            print(f"✓ {name}: Already approved")
            continue
        
        print(f"  {name}: Approving...")
        tx = getattr(contract.functions, approve_fn)(spender_addr, value).build_transaction({
            'from': address,
            'nonce': nonce,
            'gas': 60000,
            'gasPrice': gas_price,
            'chainId': 137,
        })
        
        if send_tx(w3, tx, config.PRIVATE_KEY, name):
            nonce += 1
        else:
            print(f"  Failed to approve {name}")
    
    print("\n" + "=" * 60)
    print("Approval setup complete!")


if __name__ == "__main__":
    approve_usdc()
