
import json
import paho.mqtt.client as mqtt
from kafka import KafkaProducer

# ==========================================
# KONFIGURASI
# ==========================================

MQTT_BROKER = "localhost"
MQTT_PORT = 1883

MQTT_TOPIC = "greenhouse/24.83.1103/greenhouse01/telemetry"

KAFKA_BROKER = "localhost:9092"
KAFKA_TOPIC = "greenhouse-telemetry"

# ==========================================
# KAFKA PRODUCER
# ==========================================

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda data: json.dumps(data).encode("utf-8")
)

print("[KAFKA] Producer berhasil dibuat.")

# ==========================================
# MQTT CALLBACK
# ==========================================

def on_connect(client, userdata, flags, reason_code, properties):
    if reason_code == 0:
        print("[MQTT] Terhubung ke Mosquitto.")
        client.subscribe(MQTT_TOPIC, qos=1)
        print(f"[MQTT] Subscribe: {MQTT_TOPIC}")
    else:
        print(f"[MQTT] Koneksi gagal: {reason_code}")


def on_message(client, userdata, msg):
    try:
        data = json.loads(msg.payload.decode("utf-8"))

        future = producer.send(KAFKA_TOPIC, value=data)
        metadata = future.get(timeout=10)

        print(
            f"[BRIDGE] Data dikirim | "
            f"Partition: {metadata.partition} | "
            f"Offset: {metadata.offset} | "
            f"Data: {data}"
        )

    except Exception as error:
        print(f"[ERROR] {error}")


# ==========================================
# MQTT CLIENT
# ==========================================

client = mqtt.Client(
    mqtt.CallbackAPIVersion.VERSION2,
    client_id="mqtt-kafka-bridge"
)

client.on_connect = on_connect
client.on_message = on_message

try:
    client.connect(MQTT_BROKER, MQTT_PORT, 60)

    print("[BRIDGE] MQTT -> Kafka berjalan.")
    client.loop_forever()

except KeyboardInterrupt:
    print("\n[BRIDGE] Dihentikan.")

finally:
    client.disconnect()
    producer.flush()
    producer.close()
