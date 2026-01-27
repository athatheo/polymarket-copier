#!/usr/bin/env python3
"""Swap Native USDC to Bridged USDC (USDC.e) via Uniswap V3."""

from web3 import Web3
from eth_account import Account
import os
from dotenv import load_dotenv
import time

load_dotenv()

w3 = Web3(Web3.HTTPProvider('https://polygon-rpc.com'))
pk = os.getenv('PRIVATE_KEY')
account = Account.from_key(pk)
wallet = account.address

# Token addresses
NATIVE_USDC = Web3.to_checksum_address('0x3c499c542cEF5E3811e1192ce70d8cC03d5c3359')
BRIDGED_USDC = Web3.to_checksum_address('0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174')
UNISWAP_ROUTER = Web3.to_checksum_address('0xE592427A0AEce92De3Edee1F18E0157C05861564')

# ERC20 ABI
ERC20_ABI = [
    {'inputs': [{'name': 'spender', 'type': 'address'}, {'name': 'amount', 'type': 'uint256'}], 'name': 'approve', 'outputs': [{'name': '', 'type': 'bool'}], 'stateMutability': 'nonpayable', 'type': 'function'},
    {'inputs': [{'name': 'account', 'type': 'address'}], 'name': 'balanceOf', 'outputs': [{'name': '', 'type': 'uint256'}], 'stateMutability': 'view', 'type': 'function'},
    {'inputs': [{'name': 'owner', 'type': 'address'}, {'name': 'spender', 'type': 'address'}], 'name': 'allowance', 'outputs': [{'name': '', 'type': 'uint256'}], 'stateMutability': 'view', 'type': 'function'},
]

native = w3.eth.contract(address=NATIVE_USDC, abi=ERC20_ABI)
bridged = w3.eth.contract(address=BRIDGED_USDC, abi=ERC20_ABI)

# Get balance
native_balance = native.functions.balanceOf(wallet).call()
print(f'Native USDC to swap: ${native_balance/1e6:.2f}')

if native_balance == 0:
    print('No Native USDC to swap!')
    exit(0)

# Check allowance
allowance = native.functions.allowance(wallet, UNISWAP_ROUTER).call()
print(f'Current allowance: ${allowance/1e6:.2f}')

if allowance < native_balance:
    print('')
    print('Step 1: Approving Native USDC for Uniswap...')
    
    approve_tx = native.functions.approve(
        UNISWAP_ROUTER,
        native_balance
    ).build_transaction({
        'from': wallet,
        'nonce': w3.eth.get_transaction_count(wallet),
        'gas': 100000,
        'maxFeePerGas': w3.eth.gas_price * 2,
        'maxPriorityFeePerGas': w3.eth.gas_price,
    })
    
    signed = account.sign_transaction(approve_tx)
    tx_hash = w3.eth.send_raw_transaction(signed.raw_transaction)
    print(f'Approve tx: https://polygonscan.com/tx/{tx_hash.hex()}')
    
    receipt = w3.eth.wait_for_transaction_receipt(tx_hash, timeout=60)
    if receipt['status'] == 1:
        print('Approve confirmed!')
    else:
        print('Approve FAILED!')
        exit(1)
else:
    print('Already approved, skipping...')

print('')
print('Step 2: Executing swap via Uniswap V3...')

# Uniswap V3 SwapRouter ABI
SWAP_ROUTER_ABI = [{
    'inputs': [{
        'components': [
            {'name': 'tokenIn', 'type': 'address'},
            {'name': 'tokenOut', 'type': 'address'},
            {'name': 'fee', 'type': 'uint24'},
            {'name': 'recipient', 'type': 'address'},
            {'name': 'deadline', 'type': 'uint256'},
            {'name': 'amountIn', 'type': 'uint256'},
            {'name': 'amountOutMinimum', 'type': 'uint256'},
            {'name': 'sqrtPriceLimitX96', 'type': 'uint160'}
        ],
        'name': 'params',
        'type': 'tuple'
    }],
    'name': 'exactInputSingle',
    'outputs': [{'name': 'amountOut', 'type': 'uint256'}],
    'stateMutability': 'payable',
    'type': 'function'
}]

router = w3.eth.contract(address=UNISWAP_ROUTER, abi=SWAP_ROUTER_ABI)

amount_in = native_balance
min_amount_out = int(amount_in * 0.995)  # 0.5% slippage
deadline = int(time.time()) + 300

print(f'Swapping ${amount_in/1e6:.2f} Native USDC')
print(f'Min output: ${min_amount_out/1e6:.2f} Bridged USDC')

swap_params = {
    'tokenIn': NATIVE_USDC,
    'tokenOut': BRIDGED_USDC,
    'fee': 100,  # 0.01% fee tier
    'recipient': wallet,
    'deadline': deadline,
    'amountIn': amount_in,
    'amountOutMinimum': min_amount_out,
    'sqrtPriceLimitX96': 0
}

swap_tx = router.functions.exactInputSingle(swap_params).build_transaction({
    'from': wallet,
    'nonce': w3.eth.get_transaction_count(wallet),
    'gas': 300000,
    'maxFeePerGas': w3.eth.gas_price * 2,
    'maxPriorityFeePerGas': w3.eth.gas_price,
    'value': 0
})

signed = account.sign_transaction(swap_tx)
tx_hash = w3.eth.send_raw_transaction(signed.raw_transaction)
print(f'Swap tx: https://polygonscan.com/tx/{tx_hash.hex()}')

receipt = w3.eth.wait_for_transaction_receipt(tx_hash, timeout=60)
if receipt['status'] == 1:
    print('Swap confirmed!')
    
    new_native = native.functions.balanceOf(wallet).call()
    new_bridged = bridged.functions.balanceOf(wallet).call()
    print('')
    print(f'New Native USDC: ${new_native/1e6:.2f}')
    print(f'New Bridged USDC: ${new_bridged/1e6:.2f}')
    print('')
    print('SUCCESS! You can now use the funds on Polymarket.')
else:
    print('Swap FAILED!')
    print(f'Check: https://polygonscan.com/tx/{tx_hash.hex()}')
