

# /// script
# requires-python = ">=3.10"
# dependencies = [
#     "numpy",
#     "kafka-python",
#     "python-dotenv"
# ]
# ///

import asyncio
import csv
import os
import random
import numpy as np
from datetime import datetime
from kafka import KafkaProducer
import json
from dotenv import load_dotenv

csv_path = os.path.join(os.path.dirname(
    os.path.abspath(__file__)), "hotspots.csv")

# Load environment variables from the .env file in the project root (parent folder)
load_dotenv(os.path.join(os.path.dirname(
    os.path.dirname(os.path.abspath(__file__))), ".env"))


producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    acks='all'
)


def load_hotspots():

    hotspots = {}
    with open(csv_path, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            name = row["Name"].strip().replace(" ", "_")
            hotspots[name] = {
                "lat": float(row["LatMean"]),
                "lon": float(row["LngMean"]),
                "lat_spread": float(row["LatSpread"]),
                "lon_spread": float(row["LngSpread"]),
            }
    return hotspots


# Load hotspot parameters from hotspots.csv
HOTSPOTS = load_hotspots()


async def generate_pings(name, params):
    """Generates continuous pings for a specific hotspot at random intervals."""
    mean = [params["lat"], params["lon"]]
    cov = [[params["lat_spread"]**2, 0], [0, params["lon_spread"]**2]]

    while True:
        # 1. Generate a single random GPS coordinate using Gaussian distribution
        gps_ping = np.random.multivariate_normal(mean, cov, size=1)[0]

        # 2. Create a JSON payload (this is what you would send to Kafka)
        payload = {
            "timestamp": datetime.now().isoformat(),
            "latitude": round(gps_ping[0], 6),
            "longitude": round(gps_ping[1], 6),
            "speed_kmh": round(random.uniform(10, 60), 1),
            # Gaussian-distributed GPS accuracy error (mean 15m, std 10m), clipped at 1m
            "radius_error_m": round(max(1.0, np.random.normal(15.0, 10.0)), 1)
        }

        producer.send('gps-raw', payload)

        print(f"[{name}] Generated: {json.dumps(payload)}")

        # wait for a random interval before the next ping
        random_interval = random.uniform(0.5, 3.0)
        await asyncio.sleep(random_interval)


async def main():

    print("Starting Tehran Real-Time GPS Simulator...")
    # Create an asynchronous task for each hotspot
    tasks = [generate_pings(name, params) for name, params in HOTSPOTS.items()]

    # Run all tasks concurrently
    await asyncio.gather(*tasks)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nSimulation stopped by user.")
