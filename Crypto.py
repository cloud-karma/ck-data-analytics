import time
import json
import requests
from google.cloud import pubsub_v1

# === CONFIGURATION ===
PROJECT_ID = "your-gcp-project-id"
TOPIC_ID = "crypto-price-stream"
COINGECKO_API_KEY = "your-api-key"  # From https://www.coingecko.com/en/developers/dashboard

# Coin IDs to fetch
COINS = ["bitcoin", "ethereum", "solana"]
VS_CURRENCY = "usd"
API_URL = f"https://api.coingecko.com/api/v3/simple/price"

# === Pub/Sub Publisher ===
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)

# === Data Pulling Loop ===
def fetch_and_publish():
    params = {
        "ids": ",".join(COINS),
        "vs_currencies": VS_CURRENCY,
        "x_cg_pro_api_key": COINGECKO_API_KEY,
    }

    while True:
        try:
            response = requests.get(API_URL, params=params)
            if response.status_code == 200:
                prices = response.json()
                timestamp = time.time()

                for coin, data in prices.items():
                    message = {
                        "timestamp": timestamp,
                        "coin": coin,
                        "price": data[VS_CURRENCY]
                    }
                    future = publisher.publish(
                        topic_path,
                        json.dumps(message).encode("utf-8"),
                        coin=coin
                    )
                    print(f"Published: {message}")
            else:
                print(f"Error: {response.status_code} - {response.text}")
        except Exception as e:
            print(f"Exception: {e}")

        time.sleep(10)

if __name__ == "__main__":
    fetch_and_publish()
