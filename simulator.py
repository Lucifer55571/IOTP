
import json
import random
import time
from datetime import datetime, timezone, timedelta

import paho.mqtt.client as mqtt


# ==================================================
# 1. KONFIGURASI IDENTITAS DAN MQTT
# ==================================================

NIM = "24.83.1103"

DEVICE = f"greenhouse-{NIM}"
BASE = f"greenhouse/{NIM}/greenhouse01"

BROKER = "localhost"
PORT = 1883
INTERVAL = 5

# Topic sensor
TOPIC_TEMPERATURE = f"{BASE}/sensor/temperature"
TOPIC_HUMIDITY = f"{BASE}/sensor/humidity"
TOPIC_SOIL = f"{BASE}/sensor/soil_moisture"

# Topic telemetry
TOPIC_TELEMETRY = f"{BASE}/telemetry"

# Topic actuator dan status
TOPIC_COMMAND = f"{BASE}/actuator/command"
TOPIC_ACTUATOR = f"{BASE}/actuator/state"
TOPIC_STATUS = f"{BASE}/status"


# ==================================================
# 2. KONDISI AWAL GREENHOUSE
# ==================================================

# Kondisi awal buruk
temperature = 36.0
humidity = 30.0
soil_moisture = 15.0

# Aktuator awal mati
fan = "off"
pump = "off"

connected = False


# ==================================================
# 3. TIMESTAMP WIB
# ==================================================

def get_timestamp():
    jakarta = timezone(timedelta(hours=7))

    return datetime.now(jakarta).isoformat(
        timespec="seconds"
    )


# ==================================================
# 4. FUNGSI PUBLISH JSON
# ==================================================

def publish_json(topic, data, qos=1, retain=False):

    payload = json.dumps(data)

    return client.publish(
        topic,
        payload,
        qos=qos,
        retain=retain
    )


# ==================================================
# 5. PUBLISH STATUS AKTUATOR
# ==================================================

def publish_actuator_state():

    actuator_data = {
        "device_id": DEVICE,
        "timestamp": get_timestamp(),
        "fan": fan,
        "pump": pump
    }

    publish_json(
        TOPIC_ACTUATOR,
        actuator_data,
        qos=1,
        retain=True
    )


# ==================================================
# 6. CALLBACK KONEKSI MQTT
# ==================================================

def on_connect(client, userdata, flags, reason_code, properties):

    global connected

    if reason_code == 0:

        connected = True

        print("\n[MQTT] Berhasil terhubung ke Mosquitto!")

        client.subscribe(TOPIC_COMMAND, qos=1)

        client.publish(
            TOPIC_STATUS,
            "online",
            qos=1,
            retain=True
        )

        print(f"[MQTT] Subscribe: {TOPIC_COMMAND}")

        publish_actuator_state()

    else:

        connected = False

        print(f"[MQTT] Koneksi gagal: {reason_code}")


def on_disconnect(client, userdata, disconnect_flags,
                  reason_code, properties):

    global connected

    connected = False

    print("[MQTT] Koneksi ke Mosquitto terputus.")


# ==================================================
# 7. CALLBACK PERINTAH AKTUATOR
# ==================================================

def on_message(client, userdata, msg):

    global fan, pump

    try:

        command = json.loads(
            msg.payload.decode("utf-8")
        )

        print("\n[COMMAND] Perintah diterima:")
        print(command)

        # Kontrol Fan
        if "fan" in command:

            value = str(command["fan"]).lower()

            if value in ["on", "off"]:
                fan = value

        # Kontrol Pump
        if "pump" in command:

            value = str(command["pump"]).lower()

            if value in ["on", "off"]:
                pump = value

        # Kirim status terbaru
        publish_actuator_state()

        print(
            f"[ACTUATOR] Fan: {fan.upper()} | "
            f"Pump: {pump.upper()}"
        )

    except (json.JSONDecodeError, UnicodeDecodeError, AttributeError) as error:

        print(f"[COMMAND] Format tidak valid: {error}")


# ==================================================
# 8. INISIALISASI MQTT CLIENT
# ==================================================

client = mqtt.Client(
    mqtt.CallbackAPIVersion.VERSION2,
    client_id=DEVICE
)

client.on_connect = on_connect
client.on_disconnect = on_disconnect
client.on_message = on_message

# Last Will and Testament
client.will_set(
    TOPIC_STATUS,
    "offline",
    qos=1,
    retain=True
)

client.connect(
    BROKER,
    PORT,
    60
)

client.loop_start()


# ==================================================
# 9. SIMULASI SENSOR SMART GREENHOUSE
# ==================================================

def simulate_sensor():

    global temperature
    global humidity
    global soil_moisture

    # ------------------------------------------
    # SIMULASI SUHU
    # ------------------------------------------

    if fan == "on":

        target_temperature = 26.0

        temperature += (
            (target_temperature - temperature) * 0.12
            + random.uniform(-0.15, 0.15)
        )

    else:

        target_temperature = 36.0

        temperature += (
            (target_temperature - temperature) * 0.08
            + random.uniform(-0.15, 0.15)
        )

    # ------------------------------------------
    # SIMULASI KELEMBAPAN UDARA
    # ------------------------------------------

    if fan == "on":

        # Udara menjadi lebih lembap secara bertahap
        # sebagai efek pendinginan dalam model simulasi
        target_humidity = 65.0 if pump == "on" else 55.0

    else:

        target_humidity = 30.0

    humidity += (
        (target_humidity - humidity) * 0.08
        + random.uniform(-0.3, 0.3)
    )

    humidity = max(
        0.0,
        min(100.0, humidity)
    )

    # ------------------------------------------
    # SIMULASI KELEMBAPAN TANAH
    # ------------------------------------------

    if pump == "on":

        # Tanah menjadi lebih lembap
        soil_moisture += random.uniform(2.0, 3.0)

    else:

        # Tanah semakin kering
        soil_moisture -= random.uniform(0.1, 0.3)

    soil_moisture = max(
        0.0,
        min(100.0, soil_moisture)
    )

    return (
        round(temperature, 1),
        round(humidity, 1),
        round(soil_moisture, 1)
    )


# ==================================================
# 10. PENGIRIMAN DATA SENSOR
# ==================================================

try:

    print("=" * 60)
    print("          SMART GREENHOUSE IoT SIMULATOR")
    print(f"          NIM       : {NIM}")
    print(f"          DEVICE    : {DEVICE}")
    print("=" * 60)

    print("\n[INITIAL CONDITION]")
    print("Temperature   : 36.0 °C (HIGH)")
    print("Humidity      : 30.0 % (LOW)")
    print("Soil Moisture : 15.0 % (DRY)")
    print("Fan           : OFF")
    print("Pump          : OFF")

    while True:

        if not connected:
            time.sleep(1)
            continue

        temp, hum, soil = simulate_sensor()

        timestamp = get_timestamp()

        # --------------------------------------
        # TELEMETRY GABUNGAN
        # --------------------------------------

        telemetry = {
            "device_id": DEVICE,
            "timestamp": timestamp,
            "temperature": temp,
            "humidity": hum,
            "soil_moisture": soil,
            "fan": fan,
            "pump": pump
        }

        # --------------------------------------
        # TELEMETRY SENSOR TERPISAH
        # --------------------------------------

        temperature_data = {
            "device_id": DEVICE,
            "timestamp": timestamp,
            "temperature": temp
        }

        humidity_data = {
            "device_id": DEVICE,
            "timestamp": timestamp,
            "humidity": hum
        }

        soil_data = {
            "device_id": DEVICE,
            "timestamp": timestamp,
            "soil_moisture": soil
        }

        # Publish temperature
        publish_json(
            TOPIC_TEMPERATURE,
            temperature_data
        )

        # Publish humidity
        publish_json(
            TOPIC_HUMIDITY,
            humidity_data
        )

        # Publish soil moisture
        publish_json(
            TOPIC_SOIL,
            soil_data
        )

        # Publish telemetry gabungan
        publish_json(
            TOPIC_TELEMETRY,
            telemetry
        )

        # --------------------------------------
        # TAMPILAN TERMINAL
        # --------------------------------------

        print("\n" + "-" * 60)
        print(f"Timestamp       : {timestamp}")
        print(f"Temperature     : {temp} °C")
        print(f"Humidity        : {hum} %")
        print(f"Soil Moisture   : {soil} %")
        print(f"Fan             : {fan.upper()}")
        print(f"Water Pump      : {pump.upper()}")
        print("-" * 60)

        time.sleep(INTERVAL)


except KeyboardInterrupt:

    print("\n[MQTT] Simulator dihentikan.")


finally:

    if connected:

        client.publish(
            TOPIC_STATUS,
            "offline",
            qos=1,
            retain=True
        ).wait_for_publish(timeout=2)

    client.loop_stop()
    client.disconnect()

    print("[MQTT] Koneksi ditutup.")
