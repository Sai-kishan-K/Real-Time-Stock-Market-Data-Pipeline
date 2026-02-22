import time
import json
import requests
from kafka import KafkaProducer

API_KEY = 'RQSKTQPI56GY1Z2V'  # Replace with your Alpha Vantage API key
SYMBOLS = ['IBM', 'AAPL', 'MSFT', 'TSLA', 'AMZN']
TOPIC = 'raw_api_events'

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# 1. Pass the symbol as an argument
def fetch_stock_data(symbol):
    url = f'https://www.alphavantage.co/query?function=GLOBAL_QUOTE&symbol={symbol}&apikey={API_KEY}'
    
    try:
        res = requests.get(url)
        data = res.json()
        
        # 2. Handle Alpha Vantage API Limits gracefully
        if "Information" in data or "Note" in data:
            print(f"⚠️ API Limit Reached or Warning for {symbol}: {data.get('Information', data.get('Note'))}")
            return None
            
        if "Global Quote" in data and data["Global Quote"]:
            quote = data["Global Quote"]
            
            # Defensive coding: check if price exists before converting to float
            if quote.get("05. price"):
                payload = {
                    "symbol": quote["01. symbol"],
                    "price": float(quote["05. price"]),
                    "volume": int(quote["06. volume"]),
                    "event_time": time.strftime('%Y-%m-%d %H:%M:%S') 
                }
                return payload
            else:
                print(f"Malformed data for {symbol}: {quote}")
                return None
                
    except Exception as e:
        print(f"Network error fetching {symbol}: {e}")
        
    return None

print(f"🚀 Starting Multi-Stock API Producer for: {SYMBOLS}")

while True:
    # 3. Loop through each symbol
    for symbol in SYMBOLS:
        data = fetch_stock_data(symbol)
        
        if data:
            producer.send(TOPIC, data)
            print(f"✅ Sent to Kafka: {data}")
            
        # Sleep 12 seconds between INDIVIDUAL calls (5 calls/min limit for free tier)
        time.sleep(60) 
        
    print("⏳ Finished fetching all symbols. Waiting before next batch to save API quota...")
    # Sleep 5 minutes between full batches to stretch your 25 daily calls for the demo
    time.sleep(300)