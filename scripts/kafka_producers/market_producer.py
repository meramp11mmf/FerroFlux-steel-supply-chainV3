import json, time, csv, sys, os
from datetime import datetime
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from config import TOPICS, DATA_FILES, INTERVALS, BATCH_SIZES, KAFKA_BOOTSTRAP_SERVERS

def create_producer(max_retries=5):
    for attempt in range(max_retries):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8") if k else None,
                acks="all",
                retries=3,
            )
            print(f"[OK] Connected to Kafka on attempt {attempt + 1}")
            return producer
        except NoBrokersAvailable:
            wait_time = (attempt + 1) * 5
            print(f"[WAIT] Kafka not ready, retrying in {wait_time}s... ({attempt + 1}/{max_retries})")
            time.sleep(wait_time)
    print("[FAIL] Could not connect to Kafka!")
    sys.exit(1)

def load_data():
    filepath = DATA_FILES["market"]
    if not os.path.exists(filepath):
        print(f"[FAIL] File not found: {filepath}")
        sys.exit(1)
    data = []
    with open(filepath, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            data.append(row)
    print(f"[DATA] Loaded {len(data)} records from market_data.csv")
    return data

def send_data(producer, records, topic):
    total_sent = 0
    batch_size = BATCH_SIZES["market"]
    interval = INTERVALS["market"]
    total_records = len(records)
    print(f"\n{'='*70}")
    print(f"   STEEL MARKET PRICES PRODUCER")
    print(f"   Total Records : {total_records}")
    print(f"   Batch Size    : {batch_size}")
    print(f"   Interval      : {interval}s")
    print(f"   Topic         : {topic}")
    print(f"{'='*70}\n")
    idx = 0
    cycle = 1
    try:
        while True:
            for _ in range(batch_size):
                record = records[idx % total_records]
                enriched = {
                    "event_id": f"MKT-{total_sent + 1:06d}",
                    "event_timestamp": datetime.now().isoformat(),
                    "producer": "market_prices",
                    "cycle": cycle,
                }
                enriched.update(record)
                
                key = record.get("date", "unknown")
                producer.send(topic, key=key, value=enriched)
                
                total_sent += 1
                idx += 1
            
            producer.flush() 
            print(f"   [SENT] Batch complete. Total sent: {total_sent}")
            
            if idx >= total_records * cycle:
                cycle += 1
            time.sleep(interval)
    except KeyboardInterrupt:
        print(f"\n   [STOP] Producer stopped! Total sent: {total_sent}")

def main():
    print("\n" + "="*70)
    print("   Starting Market Prices Producer...")
    print("="*70)
    producer = create_producer()
    records = load_data()
    send_data(producer, records, TOPICS["market"])
    producer.close()

if __name__ == "__main__":
    main()