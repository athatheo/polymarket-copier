#!/usr/bin/env python3
"""
Polymarket Positions Dashboard.

A localhost web dashboard that displays current positions and wallet balance,
auto-refreshing every 15 seconds.

Run: python dashboard.py
Open: http://localhost:8000
"""

import asyncio
from datetime import datetime
from contextlib import asynccontextmanager

from dotenv import load_dotenv
load_dotenv()

from fastapi import FastAPI
from fastapi.responses import HTMLResponse, JSONResponse
from web3 import Web3
from eth_account import Account

from api.data_client import DataClient
from api.clob_client import ClobClient
from storage.state import record_portfolio_snapshot, get_portfolio_history, get_portfolio_stats
import config
import httpx

# HTML template with embedded CSS and JavaScript
HTML_TEMPLATE = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Polymarket Dashboard</title>
    <style>
        * {
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }
        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, sans-serif;
            background: linear-gradient(135deg, #1a1a2e 0%, #16213e 100%);
            min-height: 100vh;
            color: #e4e4e7;
            padding: 20px;
        }
        .container {
            max-width: 1200px;
            margin: 0 auto;
        }
        header {
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 30px;
            padding-bottom: 20px;
            border-bottom: 1px solid #374151;
        }
        h1 {
            font-size: 1.8rem;
            background: linear-gradient(90deg, #8b5cf6, #06b6d4);
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
        }
        .status {
            display: flex;
            align-items: center;
            gap: 10px;
            font-size: 0.9rem;
            color: #9ca3af;
        }
        .status-dot {
            width: 8px;
            height: 8px;
            border-radius: 50%;
            background: #22c55e;
            animation: pulse 2s infinite;
        }
        .status-dot.loading {
            background: #f59e0b;
        }
        .status-dot.error {
            background: #ef4444;
            animation: none;
        }
        @keyframes pulse {
            0%, 100% { opacity: 1; }
            50% { opacity: 0.5; }
        }
        .grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(300px, 1fr));
            gap: 20px;
            margin-bottom: 30px;
        }
        .card {
            background: rgba(255, 255, 255, 0.05);
            border: 1px solid #374151;
            border-radius: 12px;
            padding: 20px;
            backdrop-filter: blur(10px);
        }
        .card-header {
            display: flex;
            align-items: center;
            gap: 10px;
            margin-bottom: 15px;
            font-size: 0.9rem;
            color: #9ca3af;
            text-transform: uppercase;
            letter-spacing: 0.5px;
        }
        .card-value {
            font-size: 2rem;
            font-weight: 700;
            color: #fff;
        }
        .card-value.positive {
            color: #22c55e;
        }
        .card-sub {
            font-size: 0.85rem;
            color: #6b7280;
            margin-top: 5px;
        }
        .positions-table {
            width: 100%;
            border-collapse: collapse;
        }
        .positions-table th,
        .positions-table td {
            text-align: left;
            padding: 12px 15px;
            border-bottom: 1px solid #374151;
        }
        .positions-table th {
            color: #9ca3af;
            font-weight: 500;
            font-size: 0.8rem;
            text-transform: uppercase;
            letter-spacing: 0.5px;
        }
        .positions-table tr:hover {
            background: rgba(255, 255, 255, 0.03);
        }
        .positions-table td:last-child {
            text-align: right;
        }
        .positions-table th:last-child {
            text-align: right;
        }
        .outcome-badge {
            display: inline-block;
            padding: 4px 10px;
            border-radius: 20px;
            font-size: 0.75rem;
            font-weight: 600;
            text-transform: uppercase;
        }
        .outcome-yes {
            background: rgba(34, 197, 94, 0.2);
            color: #22c55e;
        }
        .outcome-no {
            background: rgba(239, 68, 68, 0.2);
            color: #ef4444;
        }
        .outcome-other {
            background: rgba(139, 92, 246, 0.2);
            color: #a78bfa;
        }
        .status-badge {
            display: inline-block;
            padding: 4px 10px;
            border-radius: 20px;
            font-size: 0.75rem;
            font-weight: 600;
        }
        .status-active {
            background: rgba(59, 130, 246, 0.2);
            color: #60a5fa;
        }
        .status-won {
            background: rgba(34, 197, 94, 0.2);
            color: #22c55e;
        }
        .status-lost {
            background: rgba(239, 68, 68, 0.2);
            color: #ef4444;
        }
        .side-buy {
            background: rgba(34, 197, 94, 0.2);
            color: #22c55e;
        }
        .side-sell {
            background: rgba(239, 68, 68, 0.2);
            color: #ef4444;
        }
        .status-pending {
            background: rgba(251, 191, 36, 0.2);
            color: #fbbf24;
        }
        .two-column {
            display: grid;
            grid-template-columns: 1fr 1fr;
            gap: 20px;
            margin-top: 20px;
            margin-bottom: 20px;
        }
        @media (max-width: 900px) {
            .two-column {
                grid-template-columns: 1fr;
            }
        }
        .time-ago {
            color: #6b7280;
            font-size: 0.85rem;
        }
        .empty-state {
            text-align: center;
            padding: 40px;
            color: #6b7280;
        }
        .wallet-address {
            font-family: monospace;
            font-size: 0.85rem;
            color: #6b7280;
            word-break: break-all;
        }
        .section-title {
            font-size: 1.1rem;
            font-weight: 600;
            margin-bottom: 15px;
            color: #e4e4e7;
        }
        .refresh-info {
            text-align: center;
            padding: 15px;
            color: #6b7280;
            font-size: 0.85rem;
        }
        .error-banner {
            background: rgba(239, 68, 68, 0.1);
            border: 1px solid #ef4444;
            border-radius: 8px;
            padding: 15px;
            margin-bottom: 20px;
            color: #ef4444;
            display: none;
        }
        .error-banner.visible {
            display: block;
        }
        .chart-container {
            position: relative;
            height: 300px;
            width: 100%;
        }
        .chart-header {
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 15px;
            flex-wrap: wrap;
            gap: 10px;
        }
        .time-range-buttons {
            display: flex;
            gap: 5px;
            flex-wrap: wrap;
        }
        .time-range-btn {
            background: rgba(255, 255, 255, 0.05);
            border: 1px solid #374151;
            border-radius: 6px;
            padding: 6px 12px;
            color: #9ca3af;
            font-size: 0.8rem;
            cursor: pointer;
            transition: all 0.2s;
        }
        .time-range-btn:hover {
            background: rgba(255, 255, 255, 0.1);
            color: #e4e4e7;
        }
        .time-range-btn.active {
            background: rgba(139, 92, 246, 0.3);
            border-color: #8b5cf6;
            color: #fff;
        }
        .chart-stats {
            display: flex;
            gap: 20px;
            margin-top: 15px;
            flex-wrap: wrap;
        }
        .chart-stat {
            font-size: 0.85rem;
        }
        .chart-stat-label {
            color: #6b7280;
        }
        .chart-stat-value {
            color: #e4e4e7;
            font-weight: 600;
        }
        .chart-stat-value.positive {
            color: #22c55e;
        }
        .chart-stat-value.negative {
            color: #ef4444;
        }
    </style>
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
</head>
<body>
    <div class="container">
        <header>
            <h1>Polymarket Dashboard</h1>
            <div class="status">
                <div class="status-dot" id="statusDot"></div>
                <span id="lastUpdate">Loading...</span>
            </div>
        </header>
        
        <div class="error-banner" id="errorBanner"></div>
        
        <div class="grid">
            <div class="card">
                <div class="card-header">Total Portfolio</div>
                <div class="card-value" id="totalPortfolio">$0.00</div>
                <div class="card-sub" id="walletAddress">Loading...</div>
            </div>
            <div class="card">
                <div class="card-header">USDC.e Balance</div>
                <div class="card-value" id="usdcBalance">$0.00</div>
                <div class="card-sub">Available cash</div>
            </div>
            <div class="card">
                <div class="card-header">Positions Value</div>
                <div class="card-value" id="positionsValue">$0.00</div>
                <div class="card-sub" id="positionsCount">0 positions</div>
            </div>
            <div class="card">
                <div class="card-header">POL/MATIC</div>
                <div class="card-value" id="maticBalance">0.0000</div>
                <div class="card-sub">Gas token</div>
            </div>
        </div>
        
        <div class="card" style="margin-bottom: 20px;">
            <div class="chart-header">
                <div class="section-title">Portfolio Over Time</div>
                <div class="time-range-buttons">
                    <button class="time-range-btn active" data-range="24h">24H</button>
                    <button class="time-range-btn" data-range="3d">3D</button>
                    <button class="time-range-btn" data-range="7d">7D</button>
                    <button class="time-range-btn" data-range="1m">1M</button>
                    <button class="time-range-btn" data-range="3m">3M</button>
                    <button class="time-range-btn" data-range="6m">6M</button>
                    <button class="time-range-btn" data-range="1y">1Y</button>
                    <button class="time-range-btn" data-range="all">ALL</button>
                </div>
            </div>
            <div class="chart-container">
                <canvas id="portfolioChart"></canvas>
            </div>
            <div class="chart-stats">
                <div class="chart-stat">
                    <span class="chart-stat-label">24h Change: </span>
                    <span class="chart-stat-value" id="change24h">--</span>
                </div>
                <div class="chart-stat">
                    <span class="chart-stat-label">All-Time High: </span>
                    <span class="chart-stat-value" id="athValue">--</span>
                </div>
                <div class="chart-stat">
                    <span class="chart-stat-label">All-Time Low: </span>
                    <span class="chart-stat-value" id="atlValue">--</span>
                </div>
                <div class="chart-stat">
                    <span class="chart-stat-label">Data Points: </span>
                    <span class="chart-stat-value" id="dataPoints">0</span>
                </div>
            </div>
        </div>
        
        <div class="card">
            <div class="section-title">Active Positions</div>
            <table class="positions-table">
                <thead>
                    <tr>
                        <th>Market</th>
                        <th>Outcome</th>
                        <th>Status</th>
                        <th>Shares</th>
                        <th>Avg Price</th>
                        <th>Value</th>
                    </tr>
                </thead>
                <tbody id="positionsTableBody">
                    <tr>
                        <td colspan="6" class="empty-state">Loading positions...</td>
                    </tr>
                </tbody>
            </table>
        </div>
        
        <div class="two-column">
            <div class="card">
                <div class="section-title">Recent Trades</div>
                <table class="positions-table">
                    <thead>
                        <tr>
                            <th>Time</th>
                            <th>Market</th>
                            <th>Side</th>
                            <th>Size</th>
                            <th>Price</th>
                        </tr>
                    </thead>
                    <tbody id="tradesTableBody">
                        <tr>
                            <td colspan="5" class="empty-state">Loading trades...</td>
                        </tr>
                    </tbody>
                </table>
            </div>
            
            <div class="card">
                <div class="section-title">Open Orders</div>
                <table class="positions-table">
                    <thead>
                        <tr>
                            <th>Market</th>
                            <th>Side</th>
                            <th>Size</th>
                            <th>Price</th>
                            <th>Filled</th>
                        </tr>
                    </thead>
                    <tbody id="ordersTableBody">
                        <tr>
                            <td colspan="5" class="empty-state">Loading orders...</td>
                        </tr>
                    </tbody>
                </table>
            </div>
        </div>
        
        <div class="refresh-info">
            Auto-refreshing every 15 seconds
        </div>
    </div>
    
    <script>
        async function fetchData() {
            const statusDot = document.getElementById('statusDot');
            const lastUpdate = document.getElementById('lastUpdate');
            const errorBanner = document.getElementById('errorBanner');
            
            statusDot.classList.add('loading');
            lastUpdate.textContent = 'Updating...';
            
            try {
                const response = await fetch('/api/data');
                if (!response.ok) throw new Error('Failed to fetch data');
                
                const data = await response.json();
                
                // Update values
                document.getElementById('totalPortfolio').textContent = '$' + data.total_portfolio.toFixed(2);
                document.getElementById('usdcBalance').textContent = '$' + data.usdc_balance.toFixed(2);
                document.getElementById('positionsValue').textContent = '$' + data.positions_value.toFixed(2);
                document.getElementById('positionsCount').textContent = data.positions.length + ' position' + (data.positions.length !== 1 ? 's' : '');
                document.getElementById('maticBalance').textContent = data.matic_balance.toFixed(4);
                document.getElementById('walletAddress').textContent = data.address;
                
                // Update positions table
                const tbody = document.getElementById('positionsTableBody');
                if (data.positions.length === 0) {
                    tbody.innerHTML = '<tr><td colspan="6" class="empty-state">No active positions</td></tr>';
                } else {
                    tbody.innerHTML = data.positions.map(pos => {
                        const outcomeClass = pos.outcome.toLowerCase() === 'yes' ? 'outcome-yes' : 
                                            pos.outcome.toLowerCase() === 'no' ? 'outcome-no' : 'outcome-other';
                        
                        // Determine resolution status
                        let statusText, statusClass;
                        if (pos.redeemable) {
                            statusText = 'Won';
                            statusClass = 'status-won';
                        } else if (pos.cur_price <= 0.01 || pos.cur_price >= 0.99) {
                            statusText = 'Lost';
                            statusClass = 'status-lost';
                        } else {
                            statusText = 'Active';
                            statusClass = 'status-active';
                        }
                        
                        return `
                            <tr>
                                <td>${escapeHtml(pos.title)}</td>
                                <td><span class="outcome-badge ${outcomeClass}">${escapeHtml(pos.outcome)}</span></td>
                                <td><span class="status-badge ${statusClass}">${statusText}</span></td>
                                <td>${pos.size.toFixed(2)}</td>
                                <td>$${pos.avg_price.toFixed(4)}</td>
                                <td>$${pos.current_value.toFixed(2)}</td>
                            </tr>
                        `;
                    }).join('');
                }
                
                // Update recent trades table
                const tradesTbody = document.getElementById('tradesTableBody');
                if (!data.recent_trades || data.recent_trades.length === 0) {
                    tradesTbody.innerHTML = '<tr><td colspan="5" class="empty-state">No recent trades</td></tr>';
                } else {
                    tradesTbody.innerHTML = data.recent_trades.map(trade => {
                        const sideClass = trade.side.toLowerCase() === 'buy' ? 'side-buy' : 'side-sell';
                        return `
                            <tr>
                                <td class="time-ago">${trade.time_ago}</td>
                                <td>${escapeHtml(trade.title.substring(0, 35))}${trade.title.length > 35 ? '...' : ''}</td>
                                <td><span class="outcome-badge ${sideClass}">${trade.side}</span></td>
                                <td>${trade.size.toFixed(2)}</td>
                                <td>$${trade.price.toFixed(4)}</td>
                            </tr>
                        `;
                    }).join('');
                }
                
                // Update open orders table
                const ordersTbody = document.getElementById('ordersTableBody');
                if (!data.open_orders || data.open_orders.length === 0) {
                    ordersTbody.innerHTML = '<tr><td colspan="5" class="empty-state">No open orders</td></tr>';
                } else {
                    ordersTbody.innerHTML = data.open_orders.map(order => {
                        const sideClass = order.side.toLowerCase() === 'buy' ? 'side-buy' : 'side-sell';
                        return `
                            <tr>
                                <td>${escapeHtml(order.market.substring(0, 30))}${order.market.length > 30 ? '...' : ''}</td>
                                <td><span class="outcome-badge ${sideClass}">${order.side}</span></td>
                                <td>${order.size.toFixed(2)}</td>
                                <td>$${order.price.toFixed(4)}</td>
                                <td>${order.filled.toFixed(2)}</td>
                            </tr>
                        `;
                    }).join('');
                }
                
                // Update status
                statusDot.classList.remove('loading', 'error');
                lastUpdate.textContent = 'Updated ' + new Date().toLocaleTimeString();
                errorBanner.classList.remove('visible');
                
            } catch (error) {
                console.error('Error fetching data:', error);
                statusDot.classList.remove('loading');
                statusDot.classList.add('error');
                lastUpdate.textContent = 'Error';
                errorBanner.textContent = 'Failed to fetch data: ' + error.message;
                errorBanner.classList.add('visible');
            }
        }
        
        function escapeHtml(text) {
            const div = document.createElement('div');
            div.textContent = text;
            return div.innerHTML;
        }
        
        // Portfolio Chart
        let portfolioChart = null;
        let currentTimeRange = '24h';
        
        function initChart() {
            const ctx = document.getElementById('portfolioChart').getContext('2d');
            
            const gradient = ctx.createLinearGradient(0, 0, 0, 300);
            gradient.addColorStop(0, 'rgba(139, 92, 246, 0.3)');
            gradient.addColorStop(1, 'rgba(139, 92, 246, 0)');
            
            portfolioChart = new Chart(ctx, {
                type: 'line',
                data: {
                    labels: [],
                    datasets: [{
                        label: 'Portfolio Value',
                        data: [],
                        borderColor: '#8b5cf6',
                        backgroundColor: gradient,
                        fill: true,
                        tension: 0.4,
                        pointRadius: 0,
                        pointHoverRadius: 6,
                        pointHoverBackgroundColor: '#8b5cf6',
                        pointHoverBorderColor: '#fff',
                        pointHoverBorderWidth: 2,
                    }]
                },
                options: {
                    responsive: true,
                    maintainAspectRatio: false,
                    interaction: {
                        intersect: false,
                        mode: 'index',
                    },
                    plugins: {
                        legend: {
                            display: false
                        },
                        tooltip: {
                            backgroundColor: 'rgba(0, 0, 0, 0.8)',
                            titleColor: '#fff',
                            bodyColor: '#e4e4e7',
                            borderColor: '#374151',
                            borderWidth: 1,
                            padding: 12,
                            displayColors: false,
                            callbacks: {
                                label: function(context) {
                                    return '$' + context.parsed.y.toFixed(2);
                                }
                            }
                        }
                    },
                    scales: {
                        x: {
                            grid: {
                                color: 'rgba(55, 65, 81, 0.3)',
                            },
                            ticks: {
                                color: '#6b7280',
                                maxTicksLimit: 8,
                            }
                        },
                        y: {
                            grid: {
                                color: 'rgba(55, 65, 81, 0.3)',
                            },
                            ticks: {
                                color: '#6b7280',
                                callback: function(value) {
                                    return '$' + value.toFixed(0);
                                }
                            }
                        }
                    }
                }
            });
        }
        
        async function fetchPortfolioHistory(timeRange) {
            try {
                const response = await fetch(`/api/portfolio_history?range=${timeRange}`);
                if (!response.ok) throw new Error('Failed to fetch history');
                
                const data = await response.json();
                
                // Update chart
                if (portfolioChart && data.history) {
                    const labels = data.history.map(h => {
                        const date = new Date(h.timestamp);
                        if (timeRange === '24h') {
                            return date.toLocaleTimeString([], {hour: '2-digit', minute:'2-digit'});
                        } else if (timeRange === '3d' || timeRange === '7d') {
                            return date.toLocaleDateString([], {month: 'short', day: 'numeric', hour: '2-digit'});
                        } else {
                            return date.toLocaleDateString([], {month: 'short', day: 'numeric'});
                        }
                    });
                    
                    const values = data.history.map(h => h.total_portfolio);
                    
                    portfolioChart.data.labels = labels;
                    portfolioChart.data.datasets[0].data = values;
                    portfolioChart.update('none');
                }
                
                // Update stats
                if (data.stats) {
                    const change24hEl = document.getElementById('change24h');
                    if (data.stats.change_24h !== null) {
                        const changeStr = (data.stats.change_24h >= 0 ? '+' : '') + '$' + data.stats.change_24h.toFixed(2);
                        const pctStr = ' (' + (data.stats.change_pct_24h >= 0 ? '+' : '') + data.stats.change_pct_24h.toFixed(2) + '%)';
                        change24hEl.textContent = changeStr + pctStr;
                        change24hEl.className = 'chart-stat-value ' + (data.stats.change_24h >= 0 ? 'positive' : 'negative');
                    } else {
                        change24hEl.textContent = '--';
                        change24hEl.className = 'chart-stat-value';
                    }
                    
                    document.getElementById('athValue').textContent = data.stats.all_time_high ? '$' + data.stats.all_time_high.toFixed(2) : '--';
                    document.getElementById('atlValue').textContent = data.stats.all_time_low ? '$' + data.stats.all_time_low.toFixed(2) : '--';
                    document.getElementById('dataPoints').textContent = data.stats.total_snapshots || 0;
                }
                
            } catch (error) {
                console.error('Error fetching portfolio history:', error);
            }
        }
        
        // Time range button handlers
        document.querySelectorAll('.time-range-btn').forEach(btn => {
            btn.addEventListener('click', function() {
                document.querySelectorAll('.time-range-btn').forEach(b => b.classList.remove('active'));
                this.classList.add('active');
                currentTimeRange = this.dataset.range;
                fetchPortfolioHistory(currentTimeRange);
            });
        });
        
        // Initialize chart
        initChart();
        
        // Initial fetch
        fetchData();
        fetchPortfolioHistory(currentTimeRange);
        
        // Refresh every 15 seconds
        setInterval(fetchData, 15000);
        setInterval(() => fetchPortfolioHistory(currentTimeRange), 15000);
    </script>
</body>
</html>
"""


@asynccontextmanager
async def lifespan(app: FastAPI):
    """App lifespan - no persistent connections needed."""
    yield


app = FastAPI(
    title="Polymarket Dashboard",
    lifespan=lifespan,
)


@app.get("/", response_class=HTMLResponse)
async def index():
    """Serve the dashboard HTML."""
    return HTML_TEMPLATE


@app.get("/api/portfolio_history")
async def get_history(range: str = "24h"):
    """Get portfolio history for charting."""
    valid_ranges = ["24h", "3d", "7d", "1m", "3m", "6m", "1y", "all"]
    if range not in valid_ranges:
        range = "24h"
    
    history = get_portfolio_history(range)
    stats = get_portfolio_stats()
    
    return {
        "history": history,
        "stats": stats,
        "range": range,
    }


@app.get("/api/data")
async def get_data():
    """Fetch current positions and wallet data."""
    try:
        # Get wallet address
        if not config.PRIVATE_KEY:
            return JSONResponse(
                status_code=500,
                content={"error": "PRIVATE_KEY not configured"}
            )
        
        account = Account.from_key(config.PRIVATE_KEY)
        address = account.address
        
        # Fetch wallet balances
        w3 = Web3(Web3.HTTPProvider('https://polygon-rpc.com', request_kwargs={'timeout': 30}))
        
        # MATIC/POL balance
        matic_wei = w3.eth.get_balance(address)
        matic_balance = float(w3.from_wei(matic_wei, 'ether'))
        
        # USDC.e balance
        USDC_ADDRESS = '0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174'
        ABI = [{'constant': True, 'inputs': [{'name': '_owner', 'type': 'address'}], 
                'name': 'balanceOf', 'outputs': [{'name': 'balance', 'type': 'uint256'}], 'type': 'function'}]
        usdc_contract = w3.eth.contract(address=Web3.to_checksum_address(USDC_ADDRESS), abi=ABI)
        usdc_balance = usdc_contract.functions.balanceOf(address).call() / 1e6
        
        # Fetch positions
        data_client = DataClient()
        try:
            positions = await data_client.get_positions(address)
        finally:
            await data_client.close()
        
        # Calculate totals
        positions_value = sum(p.current_value for p in positions) if positions else 0
        total_portfolio = usdc_balance + positions_value
        
        # Format positions for JSON
        positions_data = []
        for p in (positions or []):
            # Determine status for sorting
            if p.redeemable:
                status_order = 1  # Won - second priority
            elif p.cur_price <= 0.01 or p.cur_price >= 0.99:
                status_order = 2  # Lost - last priority
            else:
                status_order = 0  # Active - first priority
            
            positions_data.append({
                "title": p.title,
                "outcome": p.outcome,
                "size": p.size,
                "avg_price": p.avg_price,
                "current_value": p.current_value,
                "cur_price": p.cur_price,
                "redeemable": p.redeemable,
                "end_date": p.end_date,
                "_status_order": status_order,
            })
        
        # Sort: Active first, then Won, then Lost (by value within each group)
        positions_data.sort(key=lambda x: (x["_status_order"], -x["current_value"]))
        
        # Remove internal sort key
        for p in positions_data:
            del p["_status_order"]
        
        # Fetch recent trades from activity API
        recent_trades = []
        try:
            async with httpx.AsyncClient(timeout=15.0) as http:
                url = f"{config.DATA_API_URL}/activity"
                params = {"user": address, "limit": 20}
                response = await http.get(url, params=params)
                if response.status_code == 200:
                    activities = response.json()
                    now = datetime.utcnow()
                    for item in activities:
                        if item.get("type") == "TRADE":
                            ts = item.get("timestamp", 0)
                            trade_time = datetime.utcfromtimestamp(ts)
                            delta = now - trade_time
                            
                            # Format time ago
                            if delta.total_seconds() < 60:
                                time_ago = f"{int(delta.total_seconds())}s ago"
                            elif delta.total_seconds() < 3600:
                                time_ago = f"{int(delta.total_seconds() / 60)}m ago"
                            elif delta.total_seconds() < 86400:
                                time_ago = f"{int(delta.total_seconds() / 3600)}h ago"
                            else:
                                time_ago = f"{int(delta.total_seconds() / 86400)}d ago"
                            
                            recent_trades.append({
                                "time_ago": time_ago,
                                "title": item.get("title", ""),
                                "side": item.get("side", ""),
                                "size": float(item.get("size", 0)),
                                "price": float(item.get("price", 0)),
                            })
                            if len(recent_trades) >= 10:
                                break
        except Exception as e:
            pass  # Silently fail for trades
        
        # Fetch open orders from CLOB API
        open_orders = []
        try:
            clob = ClobClient()
            await clob.initialize()
            orders = await clob.get_open_orders()
            for order in orders[:10]:
                open_orders.append({
                    "market": order.get("market", order.get("asset_id", "")[:20] + "..."),
                    "side": "BUY" if order.get("side") == "BUY" else "SELL",
                    "size": float(order.get("original_size", order.get("size", 0))),
                    "price": float(order.get("price", 0)),
                    "filled": float(order.get("size_matched", 0)),
                })
            await clob.close()
        except Exception as e:
            pass  # Silently fail for orders
        
        # Record portfolio snapshot for history chart
        record_portfolio_snapshot(
            total_portfolio=total_portfolio,
            usdc_balance=usdc_balance,
            positions_value=positions_value,
            matic_balance=matic_balance,
        )
        
        return {
            "address": address,
            "usdc_balance": usdc_balance,
            "matic_balance": matic_balance,
            "positions_value": positions_value,
            "total_portfolio": total_portfolio,
            "positions": positions_data,
            "recent_trades": recent_trades,
            "open_orders": open_orders,
            "timestamp": datetime.utcnow().isoformat(),
        }
        
    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={"error": str(e)}
        )


if __name__ == "__main__":
    import uvicorn
    print("\n" + "=" * 60)
    print("  Polymarket Dashboard")
    print("  Open http://localhost:8000 in your browser")
    print("  Auto-refreshes every 15 seconds")
    print("=" * 60 + "\n")
    uvicorn.run(app, host="0.0.0.0", port=8000)
