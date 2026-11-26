
import json
from kafka import KafkaConsumer

consumer = KafkaConsumer(
    "greenhouse-telemetry",
    bootstrap_servers="localhost:9092",
    group_id="greenhouse-consumer-group",
    client_id="greenhouse-consumer-2",
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    auto_commit_interval_ms=1000,
    value_deserializer=lambda data: json.loads(data.decode("utf-8"))
)

print("[CONSUMER 2] Menunggu pembagian partition...")

try:
    while True:
        records = consumer.poll(timeout_ms=1000)

        assigned = consumer.assignment()
        if assigned:
            partitions = [p.partition for p in assigned]
            print(f"[CONSUMER 2] Assigned partitions: {partitions}")

        for partition, messages in records.items():
            for message in messages:
                print(
                    f"[CONSUMER 2] "
                    f"Partition: {message.partition} | "
                    f"Offset: {message.offset} | "
                    f"Data: {message.value}"
                )

except KeyboardInterrupt:
    print("\n[CONSUMER 2] Dihentikan.")

finally:
    consumer.close()
