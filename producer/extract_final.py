#!/usr/bin/env python3
"""
PRODUCTEUR OPEN-METEO → DOSSIER (POUR FLUME)
API SIMPLE • INSERTIONS JSON • SOURCE POUR FLUME
"""

import sys
import time
import json
import os
import uuid
from datetime import datetime

print("="*60, flush=True)
print("🌦 PRODUCTEUR: Open-Meteo → Flume SpoolDir", flush=True)
print("="*60, flush=True)

# --------------------------
# CONFIGURATION
# --------------------------
API_URL = (
    "https://api.open-meteo.com/v1/forecast?"
    "latitude=51.50&longitude=-0.12&current_weather=true"
)

TARGET_RECORDS = 20000
SPOOL_DIR = "/data/flume_spool"

# Ensure spool dir exists
if not os.path.exists(SPOOL_DIR):
    os.makedirs(SPOOL_DIR)
    print(f"✅ Dossier créé: {SPOOL_DIR}")

print(f"🎯 Objectif: {TARGET_RECORDS} enregistrements", flush=True)

# --------------------------
# IMPORT MODULES
# --------------------------
try:
    import requests
    print("✅ requests OK")
except:
    print("❌ Installer requests : pip install requests")
    sys.exit(1)

# --------------------------
# REF ACTORING UTIL
# --------------------------
def transform_data(raw_json):
    """
    Extracts useful fields and adds metadata.
    """
    current = raw_json.get("current_weather", {})
    
    iso_time = current.get("time") 
    # API time is ISO8601 usually, keep as string or format standard

    record = {
        "id": str(uuid.uuid4()),
        "ingestion_timestamp": int(time.time() * 1000),
        "latitude": raw_json.get("latitude"),
        "longitude": raw_json.get("longitude"),
        "time": iso_time,
        "temperature": current.get("temperature"),
        "windspeed": current.get("windspeed"),
        "winddirection": current.get("winddirection"),
        "weathercode": current.get("weathercode"),
        "is_day": current.get("is_day")
    }
    return record

# --------------------------
# APPEL API
# --------------------------
def fetch_weather():
    try:
        r = requests.get(API_URL, timeout=10)
        if r.status_code == 200:
            return r.json()
        else:
            print(f"❌ HTTP {r.status_code}")
            return None
    except Exception as e:
        print(f"❌ API Error: {e}")
        return None

# --------------------------
# MODE FICHIER (FLUME)
# --------------------------
def save_to_spool(record):
    # Filename needs to be unique and atomic for Flume
    # Usually start with .tmp and rename, but Flume ignorePattern handles well if we are fast.
    # Safe way: write unique file
    filename = f"weather_{record['ingestion_timestamp']}_{record['id']}.json"
    filepath = os.path.join(SPOOL_DIR, filename)
    
    with open(filepath, "w") as f:
        json.dump(record, f)
    
    return 1

# --------------------------
# MAIN LOOP
# --------------------------
def main():
    print("⏳ Lancement producteur…")
    time.sleep(3)

    total = 0

    while total < TARGET_RECORDS:
        raw = fetch_weather()

        if raw:
            record = transform_data(raw)
            if record:
                save_to_spool(record)
                total += 1
                if total % 10 == 0:
                   print(f"📊 Total: {total}/{TARGET_RECORDS}", flush=True)

        # ****** MODE RAPIDE ******
        time.sleep(0.5) 

    print("\n🎉 OBJECTIF ATTEINT !!")
    print(f"Total: {total} enregistrements")


if __name__ == "__main__":
    main()
