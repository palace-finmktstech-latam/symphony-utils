#!/usr/bin/env python3
"""
Simple Trades API Backend for Symphony Bot
Serves trade data from trades.csv file

Usage:
    python trades_api.py

Endpoints:
    GET /trades/{client_id} - Returns last 5 trades for client
    GET /health - Health check
"""

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import csv
import uvicorn
from pathlib import Path
from datetime import datetime
import json

app = FastAPI(title="Trades API", description="Simple API for client trade history")

# Add CORS middleware to allow requests from anywhere (for development)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Global trades storage
TRADES = []

def load_trades_from_csv(csv_file="trades.csv"):
    """Load trades from CSV file."""
    global TRADES
    
    try:
        csv_path = Path(__file__).parent / csv_file
        
        with open(csv_path, 'r', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            TRADES = []
            
            for row in reader:
                trade = {
                    'client_id': row['client_id'].strip(),
                    'client_name': row['client_name'].strip(),
                    'trade_number': row['trade_number'].strip(),
                    'trade_date': row['trade_date'].strip(),
                    'start_date': row['start_date'].strip(),
                    'product': row['product'].strip(),
                    'direction': row['direction'].strip(),
                    'currency_pair': row['currency_pair'].strip(),
                    'notional_amount': row['notional_amount'].strip(),
                    'price': row['price'].strip(),
                    'spread': row['spread'].strip(),
                    'expiry_date': row['expiry_date'].strip()
                }
                TRADES.append(trade)
            
            print(f"✅ Loaded {len(TRADES)} trades from {csv_file}")
            return True
            
    except FileNotFoundError:
        print(f"❌ CSV file not found: {csv_file}")
        # Create minimal sample data for testing
        TRADES = [
            {
                'client_id': '76.123.456-7',
                'client_name': 'Minera Los Andes SA',
                'trade_number': 'T2025001',
                'trade_date': '06/07/2025',
                'start_date': '06/07/2025',
                'product': 'Spot',
                'direction': 'Buy',
                'currency_pair': 'USD/CLP',
                'notional_amount': '1000000',
                'price': '950.25',
                'spread': '2.5',
                'expiry_date': ''
            },
            {
                'client_id': '76.123.456-7',
                'client_name': 'Minera Los Andes SA',
                'trade_number': 'T2025002',
                'trade_date': '05/07/2025',
                'start_date': '08/07/2025',
                'product': 'Forward',
                'direction': 'Sell',
                'currency_pair': 'EUR/CLP',
                'notional_amount': '500000',
                'price': '1025.80',
                'spread': '3.0',
                'expiry_date': '08/08/2025'
            }
        ]
        print(f"⚠️ Using sample trade data: {len(TRADES)} trades")
        return False
        
    except Exception as e:
        print(f"❌ Error loading CSV: {e}")
        TRADES = []
        return False

def parse_date(date_str):
    """Parse DD/MM/YYYY date string and return as datetime for sorting."""
    try:
        return datetime.strptime(date_str, '%d/%m/%Y')
    except:
        return datetime.min

@app.get("/")
async def root():
    """Root endpoint with API info."""
    return {
        "message": "Trades API for Symphony Bot",
        "endpoints": {
            "/trades/{client_id}": "Get last 5 trades for client",
            "/health": "Health check",
            "/reload": "Reload trades from CSV"
        },
        "total_trades": len(TRADES)
    }

@app.get("/health")
async def health_check():
    """Health check endpoint."""
    return {
        "status": "healthy",
        "total_trades": len(TRADES),
        "timestamp": datetime.now().isoformat()
    }

@app.get("/trades/{client_id}")
async def get_client_trades(client_id: str):
    """Get last 5 trades for a specific client, ordered by trade_date."""
    
    print(f"📋 Request for trades: client_id={client_id}")
    
    # Find all trades for this client
    client_trades = [trade for trade in TRADES if trade['client_id'] == client_id]
    
    if not client_trades:
        print(f"❌ No trades found for client {client_id}")
        raise HTTPException(status_code=404, detail=f"No trades found for client {client_id}")
    
    # Sort by trade_date (most recent first)
    try:
        client_trades.sort(key=lambda x: parse_date(x['trade_date']), reverse=True)
    except Exception as e:
        print(f"⚠️ Warning: Could not sort trades by date: {e}")
    
    # Return last 5 trades
    last_5_trades = client_trades[:5]
    
    print(f"✅ Returning {len(last_5_trades)} trades for client {client_id}")
    
    return last_5_trades

@app.post("/reload")
async def reload_trades():
    """Reload trades from CSV file."""
    success = load_trades_from_csv("trades.csv")
    
    return {
        "success": success,
        "total_trades": len(TRADES),
        "message": "Trades reloaded successfully" if success else "Failed to reload trades - using sample data"
    }

@app.get("/stats")
async def get_stats():
    """Get statistics about loaded trades."""
    if not TRADES:
        return {"message": "No trades loaded"}
    
    # Count trades per client
    client_counts = {}
    for trade in TRADES:
        client_id = trade['client_id']
        client_counts[client_id] = client_counts.get(client_id, 0) + 1
    
    # Get unique clients
    unique_clients = len(client_counts)
    
    # Calculate average trades per client
    avg_trades = len(TRADES) / unique_clients if unique_clients > 0 else 0
    
    return {
        "total_trades": len(TRADES),
        "unique_clients": unique_clients,
        "avg_trades_per_client": round(avg_trades, 2),
        "clients_with_most_trades": sorted(client_counts.items(), key=lambda x: x[1], reverse=True)[:5]
    }

if __name__ == "__main__":
    print("🚀 Starting Trades API Server...")
    
    # Load trades data on startup
    load_trades_from_csv("trades.csv")
    
    print("📡 Server will be available at:")
    print("   - Main API: http://127.0.0.1:8001")
    print("   - Health: http://127.0.0.1:8001/health")
    print("   - Stats: http://127.0.0.1:8001/stats")
    print("   - Example: http://127.0.0.1:8001/trades/76.123.456-7")
    print()
    
    # Start the server
    uvicorn.run(app, host="127.0.0.1", port=8001, log_level="info")