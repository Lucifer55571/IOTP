
import json
import os
import time
import paho.mqtt.client as mqtt

# ==================================================
# KONFIGURASI
# ==================================================
TOKEN = os.environ["hUi2SP6OV3fkmwdy6lUb"]

NIM = "24.83.1103"
BASE = f"greenhouse/{NIM}/greenhouse01"

TB_TELEMETRY = "v1/devices/me/telemetry"
TB_RPC = "v1/devices/me/rpc/request/+"

# ==================================================
# KONEKSI THINGSBOARD
# ==================================================
def tb_on_connect(client, userdata, flags, reason_code, properties):
    print("Koneksi ThingsBoard:", reason_code)

    if reason_code == 0:
        client.subscribe(TB_RPC, qos=1)
        print("Menunggu perintah RPC dari ThingsBoard...")


def tb_on_disconnect(client, userdata, disconnect_flags, reason_code, properties):
    print("ThingsBoard terputus:", reason_code)


tb = mqtt.Client(
    mqtt.CallbackAPIVersion.VERSION2,
    client_id=f"{NIM}-bridge-tb-{int(time.time())}"
)

tb.username_pw_set(TOKEN)
tb.on_connect = tb_on_connect
tb.on_disconnect = tb_on_disconnect

tb.connect("localhost", 1884, 60)
tb.loop_start()


# ==================================================
# KONEKSI MOSQUITTO
# ==================================================
def on_connect(client, userdata, flags, reason_code, properties):
    print("Koneksi Mosquitto:", reason_code)

    if reason_code == 0:
        client.subscribe([
            (f"{BASE}/telemetry", 1),
            (f"{BASE}/actuator/state", 1)
        ])


# ==================================================
# DATA DARI MOSQUITTO KE THINGSBOARD
# ==================================================
def on_message(client, userdata, msg):
    try:
        d = json.loads(msg.payload.decode("utf-8"))

        if msg.topic == f"{BASE}/telemetry":
            telemetry = {
                "temperature": d["temperature"],
                "humidity": d["humidity"],
                "soil_moisture": d["soil_moisture"],
                "fan": d["fan"],
                "pump": d["pump"]
            }

        elif msg.topic == f"{BASE}/actuator/state":
            telemetry = {
                "fan": d["fan"],
                "pump": d["pump"]
            }

        else:
            return

        tb.publish(
            TB_TELEMETRY,
            json.dumps(telemetry),
            qos=1
        )

        print("Telemetry diteruskan:", telemetry)

    except (json.JSONDecodeError, KeyError) as e:
        print("Gagal membaca data:", e)


# ==================================================
# PERINTAH DARI THINGSBOARD KE MOSQUITTO
# ==================================================
def tb_on_message(client, userdata, msg):
    try:
        request_id = msg.topic.split("/")[-1]
        request = json.loads(msg.payload.decode("utf-8"))

        method = request.get("method")
        params = request.get("params")

        command = {}

        if method == "setFan":
            command["fan"] = "on" if params is True else "off"

        elif method == "setPump":
            command["pump"] = "on" if params is True else "off"

        else:
            print("RPC tidak dikenal:", method)
            response = {"success": False, "error": "Unknown method"}

            tb.publish(
                f"v1/devices/me/rpc/response/{request_id}",
                json.dumps(response),
                qos=1
            )
            return

        # Kirim perintah ke simulator melalui Mosquitto
        result = src.publish(
            f"{BASE}/actuator/command",
            json.dumps(command),
            qos=1
        )

        response = {
            "success": result.rc == mqtt.MQTT_ERR_SUCCESS,
            "command": command
        }

        tb.publish(
            f"v1/devices/me/rpc/response/{request_id}",
            json.dumps(response),
            qos=1
        )

        print("Perintah dari ThingsBoard:", command)

    except (json.JSONDecodeError, ValueError) as e:
        print("Gagal memproses RPC:", e)


tb.on_message = tb_on_message


# ==================================================
# MENJALANKAN MOSQUITTO CLIENT
# ==================================================
src = mqtt.Client(
    mqtt.CallbackAPIVersion.VERSION2,
    client_id=f"{NIM}-bridge-src"
)

src.on_connect = on_connect
src.on_message = on_message

src.connect("localhost", 1883, 60)

print("Smart Greenhouse MQTT Bridge berjalan...")

try:
    src.loop_forever()

except KeyboardInterrupt:
    print("Bridge dihentikan.")

finally:
    src.disconnect()
    tb.loop_stop()
    tb.disconnect()
