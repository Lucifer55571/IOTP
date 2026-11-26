
import json
import time
from kafka import KafkaConsumer

consumer = KafkaConsumer(
    "greenhouse-telemetry",
    bootstrap_servers="localhost:9092",
    group_id="greenhouse-consumer-group",
    client_id="greenhouse-consumer-1",
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    auto_commit_interval_ms=1000,
    value_deserializer=lambda data: json.loads(data.decode("utf-8"))
)

print("[CONSUMER 1] Menunggu pembagian partition...")

try:
    while True:
        records = consumer.poll(timeout_ms=1000)

        assigned = consumer.assignment()
        if assigned:
            partitions = [p.partition for p in assigned]
            print(f"[CONSUMER 1] Assigned partitions: {partitions}")

        for partition, messages in records.items():
            for message in messages:
                print(
                    f"[CONSUMER 1] "
                    f"Partition: {message.partition} | "
                    f"Offset: {message.offset} | "
                    f"Data: {message.value}"
                )

except KeyboardInterrupt:
    print("\n[CONSUMER 1] Dihentikan.")

finally:
    consumer.close()
