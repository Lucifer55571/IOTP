
import json
import time
import sqlite3
import threading
import requests
import statistics
from datetime import datetime, timezone, timedelta

import paho.mqtt.client as mqtt

# ==================================================
# 1. KONFIGURASI
# ==================================================

MQTT_BROKER = "localhost"
MQTT_PORT = 1883

MQTT_TOPIC = "greenhouse/24.83.1103/greenhouse01/telemetry"

# Ganti dengan Access Token perangkat ThingsBoard
TB_TOKEN = "hUi2SP6OV3fkmwdy6lUb"

TB_URL = f"http://localhost:8080/api/v1/{TB_TOKEN}/telemetry"

DATABASE = "edge_storage.db"

AGGREGATION_INTERVAL = 60

# Ambang batas greenhouse
TEMP_LIMIT = 32
HUMIDITY_LIMIT = 40
SOIL_LIMIT = 25

samples = []
samples_lock = threading.Lock()

# ==================================================
# 2. DATABASE SQLITE
# ==================================================

db = sqlite3.connect(DATABASE, check_same_thread=False)

db.execute("""
CREATE TABLE IF NOT EXISTS pending_data (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    payload TEXT NOT NULL
)
""")

db.commit()

# ==================================================
# 3. WAKTU WIB
# ==================================================

def get_timestamp():
    jakarta = timezone(timedelta(hours=7))

    return datetime.now(jakarta).isoformat(
        timespec="seconds"
    )

# ==================================================
# 4. SIMPAN DATA JIKA CLOUD TERPUTUS
# ==================================================

def save_offline(data):

    db.execute(
        "INSERT INTO pending_data (payload) VALUES (?)",
        (json.dumps(data),)
    )

    db.commit()

    print("[STORE] Data disimpan secara lokal.")

# ==================================================
# 5. KIRIM DATA KE THINGSBOARD
# ==================================================

def send_to_cloud(data):

    try:

        response = requests.post(
            TB_URL,
            json=data,
            timeout=5
        )

        if response.status_code == 200:

            print("[CLOUD] Data berhasil dikirim ke ThingsBoard.")
            return True

        print(
            f"[CLOUD] Gagal mengirim: "
            f"HTTP {response.status_code}"
        )

        return False

    except requests.RequestException as error:

        print(f"[CLOUD] Cloud tidak dapat dihubungi: {error}")
        return False

# ==================================================
# 6. STORE & FORWARD
# ==================================================

def forward_pending():

    cursor = db.execute(
        "SELECT id, payload FROM pending_data ORDER BY id"
    )

    records = cursor.fetchall()

    if not records:
        return

    print(f"[FORWARD] Mengirim ulang {len(records)} data...")

    for record_id, payload in records:

        data = json.loads(payload)

        if send_to_cloud(data):

            db.execute(
                "DELETE FROM pending_data WHERE id = ?",
                (record_id,)
            )

            db.commit()

            print(f"[FORWARD] Data ID {record_id} berhasil dikirim.")

        else:

            print("[FORWARD] Pengiriman dihentikan sementara.")
            break

# ==================================================
# 7. EDGE PROCESSING DAN AGREGASI
# ==================================================

def process_samples(data_list):

    if not data_list:
        return

    temperatures = [x["temperature"] for x in data_list]
    humidities = [x["humidity"] for x in data_list]
    soil_values = [x["soil_moisture"] for x in data_list]

    avg_temperature = round(statistics.mean(temperatures), 2)
    avg_humidity = round(statistics.mean(humidities), 2)
    avg_soil = round(statistics.mean(soil_values), 2)

    # Deteksi kondisi secara lokal
    alerts = []

    if avg_temperature > TEMP_LIMIT:
        alerts.append("HIGH_TEMPERATURE")

    if avg_humidity < HUMIDITY_LIMIT:
        alerts.append("LOW_HUMIDITY")

    if avg_soil < SOIL_LIMIT:
        alerts.append("LOW_SOIL_MOISTURE")

    edge_data = {
        "device_id": data_list[-1].get("device_id"),
        "timestamp": get_timestamp(),

        "temperature": avg_temperature,
        "humidity": avg_humidity,
        "soil_moisture": avg_soil,

        "fan": data_list[-1].get("fan", "off"),
        "pump": data_list[-1].get("pump", "off"),

        "sample_count": len(data_list),
        "edge_alert": len(alerts) > 0,
        "alert_reason": ", ".join(alerts) if alerts else "NORMAL"
    }

    print("\n========== EDGE PROCESSING ==========")
    print(f"Jumlah data       : {len(data_list)}")
    print(f"Rata-rata suhu    : {avg_temperature} °C")
    print(f"Rata-rata humidity: {avg_humidity} %")
    print(f"Rata-rata tanah   : {avg_soil} %")
    print(f"Status alert      : {edge_data['alert_reason']}")
    print("=====================================")

    if alerts:
        print("[LOCAL ALERT] PERINGATAN KONDISI GREENHOUSE!")

    # Coba kirim data baru
    if not send_to_cloud(edge_data):

        # Jika gagal, simpan ke database lokal
        save_offline(edge_data)

    else:

        # Jika koneksi pulih, kirim data lama
        forward_pending()

# ==================================================
# 8. MQTT CALLBACK
# ==================================================

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

        with samples_lock:
            samples.append(data)

    except Exception as error:

        print(f"[MQTT] Kesalahan data: {error}")

# ==================================================
# 9. MQTT CLIENT
# ==================================================

client = mqtt.Client(
    mqtt.CallbackAPIVersion.VERSION2,
    client_id="greenhouse-edge-processor"
)

client.on_connect = on_connect
client.on_message = on_message

# ==================================================
# 10. PROGRAM UTAMA
# ==================================================

try:

    print("=" * 50)
    print("       SMART GREENHOUSE EDGE PROCESSOR")
    print("=" * 50)

    client.connect(MQTT_BROKER, MQTT_PORT, 60)
    client.loop_start()

    # Coba kirim data tersimpan saat program dimulai
    forward_pending()

    last_aggregation = time.monotonic()

    while True:

        time.sleep(1)

        if time.monotonic() - last_aggregation >= AGGREGATION_INTERVAL:

            with samples_lock:
                current_samples = samples.copy()
                samples.clear()

            if current_samples:
                process_samples(current_samples)

            else:
                print("[EDGE] Tidak ada data pada periode ini.")

            last_aggregation = time.monotonic()

except KeyboardInterrupt:

    print("\n[EDGE] Processor dihentikan.")

finally:

    client.loop_stop()
    client.disconnect()
    db.close()

    print("[EDGE] Koneksi ditutup.")
