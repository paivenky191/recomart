import pandas as pd
import requests
import random
import logging
from datetime import datetime, timedelta

# 1. Setup Logging for Task 2 Deliverables
logging.basicConfig(
    filename='ingestion_audit.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def generate_linked_interactions(output_file="source_data/interactions.csv"):
    try:
        # 2. Fetch real product IDs from the API
        logging.info("Fetching product IDs from FakeStoreAPI...")
        response = requests.get("https://fakestoreapi.com/products")
        response.raise_for_status()
        products = response.json()
        product_ids = [p['id'] for p in products]
        logging.info(f"Successfully retrieved {len(product_ids)} product IDs.")

        # 3. Generate synthetic users and events
        users = [f"U{i:04d}" for i in range(1, 501)] # 500 users
        events = ["view"] * 60 + ["click"] * 25 + ["add_to_cart"] * 10 + ["purchase"] * 5
        devices = ["mobile_app", "desktop_browser"]
        
        data = []
        start_date = datetime.now() - timedelta(days=30)

        logging.info(f"Generating 10,000 interactions for RecoMart...")
        for i in range(10000):
            timestamp = start_date + timedelta(seconds=random.randint(0, 2592000))
            data.append({
                "interaction_id": f"INT_{i:06d}",
                "user_id": random.choice(users),
                "product_id": random.choice(product_ids), # Using real API IDs
                "event_type": random.choice(events),
                "device": random.choice(devices),
                "timestamp": timestamp.strftime('%Y-%m-%d %H:%M:%S'),
                "session_id": f"SESS_{random.randint(10000, 99999)}"
            })

        # 4. Save to CSV
        df = pd.DataFrame(data).sort_values("timestamp")
        df.to_csv(output_file, index=False)
        logging.info(f"Successfully saved linked interactions to {output_file}")
        print(f"✅ Created {output_file} with {len(df)} rows linked to API products.")

    except Exception as e:
        logging.error(f"Ingestion failed: {e}")
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    generate_linked_interactions()