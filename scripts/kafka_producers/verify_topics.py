import json, sys, os
from kafka import KafkaConsumer

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from config import TOPICS, KAFKA_BOOTSTRAP_SERVERS

def verify():
    print("\n" + "="*55)
    print("   KAFKA TOPICS VERIFICATION")
    print("="*55 + "\n")
    
    topic_list = list(TOPICS.values())
    
    for topic in topic_list:
        try:
            consumer = KafkaConsumer(
                topic,
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                auto_offset_reset="earliest",
                consumer_timeout_ms=5000,
                value_deserializer=lambda m: json.loads(m.decode("utf-8"))
            )
            
            messages = []
            for msg in consumer:
                messages.append(msg)
                if len(messages) >= 3:
                    break
            
            consumer.close()
            
            if messages:
                print(f"  [OK] {topic}")
                print(f"       Messages found: {len(messages)}+")
                print(f"       Partitions used: {set(m.partition for m in messages)}")
                print()
            else:
                print(f"  [WARN] {topic} - No messages yet (empty)")
                print()
                
        except Exception as e:
            print(f"  [FAIL] {topic} - Error: {e}")
            print()
            
    print("="*55)
    print("  Verification complete!")
    print("="*55)

if __name__ == "__main__":
    verify()