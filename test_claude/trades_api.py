#!/usr/bin/env python3
"""
Enhanced Trades API Backend for Symphony Bot
Serves trade data and client status from CSV files

Usage:
    python trades_api.py

Endpoints:
    GET /trades/{client_id} - Returns last 5 trades for client
    GET /status/{client_id} - Returns client status with traffic lights
    GET /health - Health check
    GET /stats - API statistics
"""

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import csv
import uvicorn
from pathlib import Path
from datetime import datetime
import json

app = FastAPI(title="Trades & Status API", description="API for client trade history and status")

# Add CORS middleware to allow requests from anywhere (for development)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Global storage
TRADES = []
CLIENT_STATUS = []

def safe_get(row, key, default=""):
    """Safely get value from CSV row, handling None values."""
    value = row.get(key, default)
    if value is None:
        return default
    return str(value).strip() if value else default

def load_trades_from_csv(csv_file="trades.csv"):
    """Load trades from CSV file with proper null handling."""
    global TRADES
    
    try:
        csv_path = Path(__file__).parent / csv_file
        
        with open(csv_path, 'r', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            TRADES = []
            
            for row_num, row in enumerate(reader, start=2):
                try:
                    trade = {
                        'trade_number': safe_get(row, 'trade_number'),
                        'client_id': safe_get(row, 'client_id'),
                        'client_name': safe_get(row, 'client_name'),
                        'trade_date': safe_get(row, 'trade_date'),
                        'start_date': safe_get(row, 'start_date'),
                        'product': safe_get(row, 'product'),
                        'direction': safe_get(row, 'direction'),
                        'currency_pair': safe_get(row, 'currency_pair'),
                        'notional_amount': safe_get(row, 'notional_amount'),
                        'price': safe_get(row, 'price'),
                        'spread': safe_get(row, 'spread'),
                        'expiry_date': safe_get(row, 'expiry_date', "")
                    }
                    
                    # Skip rows with missing critical data
                    if not trade['trade_number'] or not trade['client_id']:
                        print(f"⚠️  Skipping trade row {row_num}: Missing trade_number or client_id")
                        continue
                    
                    TRADES.append(trade)
                    
                except Exception as e:
                    print(f"⚠️  Error processing trade row {row_num}: {e}")
                    continue
            
            print(f"✅ Loaded {len(TRADES)} trades from {csv_file}")
            return True
            
    except FileNotFoundError:
        print(f"❌ CSV file not found: {csv_file}")
        # Create minimal sample data for testing
        TRADES = [
            {
                'client_id': '93.685.712-6',
                'client_name': 'Comercial Metropolitana SA',
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
                'client_id': '93.685.712-6',
                'client_name': 'Comercial Metropolitana SA',
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
        print(f"❌ Error loading trades CSV: {e}")
        TRADES = []
        return False

def load_client_status_from_csv(csv_file="client_status.csv"):
    """Load client status from CSV file."""
    global CLIENT_STATUS
    
    try:
        csv_path = Path(__file__).parent / csv_file
        
        with open(csv_path, 'r', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            CLIENT_STATUS = []
            
            for row_num, row in enumerate(reader, start=2):
                try:
                    status = {
                        'client_id': safe_get(row, 'client_id'),
                        'client_name': safe_get(row, 'client_name'),
                        'kyc_status': safe_get(row, 'kyc_status', 'Unknown'),
                        'onboarding_status': safe_get(row, 'onboarding_status', 'Unknown'),
                        'ccg_status': safe_get(row, 'ccg_status', 'Unknown'),
                        'contract_status': safe_get(row, 'contract_status', 'Unknown'),
                        'client_status': safe_get(row, 'client_status', 'Unknown')
                    }
                    
                    # Skip rows with missing critical data
                    if not status['client_id']:
                        print(f"⚠️  Skipping status row {row_num}: Missing client_id")
                        continue
                    
                    CLIENT_STATUS.append(status)
                    
                except Exception as e:
                    print(f"⚠️  Error processing status row {row_num}: {e}")
                    continue
            
            print(f"✅ Loaded {len(CLIENT_STATUS)} client statuses from {csv_file}")
            return True
            
    except FileNotFoundError:
        print(f"❌ CSV file not found: {csv_file}")
        # Create sample status data for testing
        CLIENT_STATUS = [
            {
                'client_id': '93.685.712-6',
                'client_name': 'Comercial Metropolitana SA',
                'kyc_status': 'OK',
                'onboarding_status': 'En Curso',
                'ccg_status': 'OK',
                'contract_status': 'NOK',
                'client_status': 'En Curso'
            }
        ]
        print(f"⚠️ Using sample status data: {len(CLIENT_STATUS)} statuses")
        return False
        
    except Exception as e:
        print(f"❌ Error loading status CSV: {e}")
        CLIENT_STATUS = []
        return False

def status_to_emoji(status):
    """Convert status text to emoji traffic light."""
    status_lower = status.lower()
    if status_lower == 'ok':
        return '🟢'
    elif status_lower == 'en curso':
        return '🟡'
    elif status_lower == 'nok':
        return '🔴'
    else:
        return '⚪'  # Unknown status

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
        "message": "Trades & Status API for Symphony Bot",
        "endpoints": {
            "/trades/{client_id}": "Get last 5 trades for client",
            "/status/{client_id}": "Get client status with traffic lights",
            "/health": "Health check",
            "/stats": "API statistics",
            "/reload": "Reload data from CSV files"
        },
        "data_loaded": {
            "total_trades": len(TRADES),
            "total_statuses": len(CLIENT_STATUS)
        }
    }

@app.get("/health")
async def health_check():
    """Health check endpoint."""
    return {
        "status": "healthy",
        "total_trades": len(TRADES),
        "total_client_statuses": len(CLIENT_STATUS),
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

@app.get("/status/{client_id}")
async def get_client_status(client_id: str):
    """Get client status with traffic light emojis."""
    
    print(f"🚦 Request for status: client_id={client_id}")
    
    # Find status for this client
    client_status = None
    for status in CLIENT_STATUS:
        if status['client_id'] == client_id:
            client_status = status
            break
    
    if not client_status:
        print(f"❌ No status found for client {client_id}")
        # Return unknown status for all fields
        client_status = {
            'client_id': client_id,
            'client_name': 'Unknown',
            'kyc_status': 'Unknown',
            'onboarding_status': 'Unknown',
            'ccg_status': 'Unknown',
            'contract_status': 'Unknown',
            'client_status': 'Unknown'
        }
    
    # Convert statuses to emojis
    status_with_emojis = {
        'client_id': client_status['client_id'],
        'client_name': client_status['client_name'],
        'kyc_status': client_status['kyc_status'],
        'kyc_emoji': status_to_emoji(client_status['kyc_status']),
        'onboarding_status': client_status['onboarding_status'],
        'onboarding_emoji': status_to_emoji(client_status['onboarding_status']),
        'ccg_status': client_status['ccg_status'],
        'ccg_emoji': status_to_emoji(client_status['ccg_status']),
        'contract_status': client_status['contract_status'],
        'contract_emoji': status_to_emoji(client_status['contract_status']),
        'client_status': client_status['client_status'],
        'client_emoji': status_to_emoji(client_status['client_status']),
        'status_line': f"{status_to_emoji(client_status['kyc_status'])} KYC  {status_to_emoji(client_status['onboarding_status'])} Onboarding  {status_to_emoji(client_status['ccg_status'])} CCG  {status_to_emoji(client_status['contract_status'])} Contract  {status_to_emoji(client_status['client_status'])} Client"
    }
    
    print(f"✅ Returning status for client {client_id}: {status_with_emojis['status_line']}")
    
    return status_with_emojis

@app.post("/reload")
async def reload_data():
    """Reload both trades and status data from CSV files."""
    trades_success = load_trades_from_csv("trades.csv")
    status_success = load_client_status_from_csv("client_status.csv")
    
    return {
        "trades_success": trades_success,
        "status_success": status_success,
        "total_trades": len(TRADES),
        "total_statuses": len(CLIENT_STATUS),
        "message": f"Reload completed - Trades: {'✅' if trades_success else '❌'}, Status: {'✅' if status_success else '❌'}"
    }

@app.get("/stats")
async def get_stats():
    """Get statistics about loaded data."""
    if not TRADES and not CLIENT_STATUS:
        return {"message": "No data loaded"}
    
    stats = {
        "trades": {
            "total_trades": len(TRADES),
            "unique_clients_with_trades": 0,
            "avg_trades_per_client": 0
        },
        "status": {
            "total_statuses": len(CLIENT_STATUS),
            "status_breakdown": {}
        }
    }
    
    # Trades statistics
    if TRADES:
        client_counts = {}
        for trade in TRADES:
            client_id = trade['client_id']
            client_counts[client_id] = client_counts.get(client_id, 0) + 1
        
        stats["trades"]["unique_clients_with_trades"] = len(client_counts)
        stats["trades"]["avg_trades_per_client"] = round(len(TRADES) / len(client_counts), 2) if client_counts else 0
        stats["trades"]["clients_with_most_trades"] = sorted(client_counts.items(), key=lambda x: x[1], reverse=True)[:5]
    
    # Status statistics
    if CLIENT_STATUS:
        for field in ['kyc_status', 'onboarding_status', 'ccg_status', 'contract_status', 'client_status']:
            field_stats = {}
            for status in CLIENT_STATUS:
                value = status.get(field, 'Unknown')
                field_stats[value] = field_stats.get(value, 0) + 1
            stats["status"]["status_breakdown"][field] = field_stats
    
    return stats

if __name__ == "__main__":
    print("🚀 Starting Enhanced Trades & Status API Server...")
    
    # Load data on startup
    print("\n📊 Loading data files...")
    load_trades_from_csv("trades.csv")
    load_client_status_from_csv("client_status.csv")
    
    print("\n📡 Server will be available at:")
    print("   - Main API: http://127.0.0.1:8001")
    print("   - Health: http://127.0.0.1:8001/health")
    print("   - Stats: http://127.0.0.1:8001/stats")
    print("   - Example trades: http://127.0.0.1:8001/trades/93.685.712-6")
    print("   - Example status: http://127.0.0.1:8001/status/93.685.712-6")
    print()
    
    # Start the server
    uvicorn.run(app, host="127.0.0.1", port=8001, log_level="info")