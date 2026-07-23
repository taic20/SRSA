import paho.mqtt.client as mqtt
import json
import time
import socket
import sys
from datetime import datetime

# --- Configurações ---
MQTT_BROKER = "127.0.0.1"
MQTT_PORT = 1883
UDP_TARGET_IP = "127.0.0.1"
UDP_TARGET_PORT = 9090
GROUP_ID = None
STALL_TIMEOUT = 30  # segundos sem mover

# --- Estado interno ---
robot_data = {}  # { "AMR-1": { "status": "...", "battery": 100, "location": "...", "last_move_time": ts, "alert_sent_stall": False } }
parking_availability = {}  # { "P1": "AVAILABLE", "P2": "AVAILABLE", "P3": "AVAILABLE" }

# --- UDP Client ---
def send_alert(robot_id, level, override_task):
    message = {
        "robot_id": robot_id,
        "level": level,
        "override_task": override_task
    }
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.sendto(json.dumps(message).encode('utf-8'), (UDP_TARGET_IP, UDP_TARGET_PORT))
        sock.close()
        print(f"[ALERTA ENVIADO] {robot_id} -> {level} ({override_task})")
    except Exception as e:
        print(f"[ERRO] Falha ao enviar alerta UDP: {e}")

# --- MQTT Callbacks ---
def on_connect(client, userdata, flags, rc):
    """Callback chamado quando a conexão com o broker é estabelecida."""
    if rc == 0:
        print("[MQTT] Conectado ao broker")
        # Subscreve a robots e informação de parking (via robots)
        topic = f"{GROUP_ID}/internal/amr/+/status"
        client.subscribe(topic, qos=1)
        print(f"[MQTT] Subscrito em: {topic}")
        
        # Inicializa parking stations como AVAILABLE
        for p in ['P1', 'P2', 'P3']:
            parking_availability[p] = "AVAILABLE"
    else:
        print(f"[ERRO] Falha na conexão MQTT: {rc}")

def on_message(client, userdata, msg):
    """Callback chamado quando uma mensagem é recebida."""
    try:
        payload = json.loads(msg.payload.decode('utf-8'))
        robot_id = payload.get("robot_id")
        status = payload.get("status")
        battery = payload.get("battery")
        location = payload.get("location_id")
        now = time.time()

        if not robot_id:
            return

        # Inicializa se for a primeira vez
        if robot_id not in robot_data:
            robot_data[robot_id] = {
                "status": status,
                "battery": battery,
                "location": location,
                "last_move_time": now,
                "alert_sent_stall": False,
                "alert_sent_battery": False
            }
            return

        prev = robot_data[robot_id]
        robot_data[robot_id].update({
            "status": status,
            "battery": battery,
            "location": location
        })

        # --- Regra 1: STALLED ---
        if status.startswith("MOVING"):
            if location != prev["location"]:
                robot_data[robot_id]["last_move_time"] = now
                robot_data[robot_id]["alert_sent_stall"] = False
            else:
                if now - prev["last_move_time"] > STALL_TIMEOUT:
                    if not robot_data[robot_id]["alert_sent_stall"]:
                        print(f"[CRITICAL] {robot_id} STALLED (30s sem mover)")
                        send_alert(robot_id, "CRITICAL", "FORCE_CHARGE")
                        robot_data[robot_id]["alert_sent_stall"] = True
                        
        elif status == "STALLED":
            if now - prev["last_move_time"] > STALL_TIMEOUT:
                if not robot_data[robot_id]["alert_sent_stall"]:
                    print(f"[CRITICAL] {robot_id} STALLED (30s sem mover)")
                    send_alert(robot_id, "CRITICAL", "FORCE_CHARGE")
                    robot_data[robot_id]["alert_sent_stall"] = True
        else:
            robot_data[robot_id]["alert_sent_stall"] = False
            robot_data[robot_id]["last_move_time"] = now

        # --- Regra 2: LOW BATTERY ---
        if battery < 15 and status not in ["CHARGING", "MOVING_TO_CHARGE"]:
            if not robot_data[robot_id]["alert_sent_battery"]:
                print(f"[CRITICAL] {robot_id} BATERIA BAIXA ({battery}%)")
                send_alert(robot_id, "CRITICAL", "FORCE_CHARGE")
                robot_data[robot_id]["alert_sent_battery"] = True
        else:
            robot_data[robot_id]["alert_sent_battery"] = False

        # --- Regra 3: PARKING STATIONS ---
        # Quando um robot está DROPPING numa parking station, fica BLOCKED
        if status == "DROPPING" and location in ['P1', 'P2', 'P3']:
            if parking_availability[location] != "BLOCKED":
                print(f"[PARKING] {robot_id} ocupando {location} → BLOCKED")
                parking_availability[location] = "BLOCKED"
                
        # Quando o robot deixa DROPPING ou sai da parking station, volta a AVAILABLE
        elif (prev["status"] == "DROPPING" and status != "DROPPING") or \
             (prev["location"] in ['P1', 'P2', 'P3'] and location != prev["location"]):
            station_id = prev["location"]
            if station_id in parking_availability and parking_availability[station_id] == "BLOCKED":
                print(f"[PARKING] {robot_id} libertou {station_id} → AVAILABLE")
                parking_availability[station_id] = "AVAILABLE"

    except Exception as e:
        print(f"[ERRO] Processamento MQTT: {e}")

# --- Função para o Fleet Coordinator consultar ---
def get_parking_status(station_id):
    """Devolve o estado atual de uma parking station."""
    return parking_availability.get(station_id, "AVAILABLE")

# --- Função para iniciar o System Monitor numa thread ---
def start_monitor():
    """Inicia o System Monitor (chamado pelo Fleet Coordinator)."""
    global client
    client = mqtt.Client(client_id=f"system_monitor_{GROUP_ID}")
    client.on_connect = on_connect
    client.on_message = on_message
    
    try:
        client.connect(MQTT_BROKER, MQTT_PORT, 60)
        client.loop_start()
        print("--- System Monitor Thread Iniciada ---")
    except Exception as e:
        print(f"[ERRO] Falha ao conectar MQTT na thread: {e}")

# --- Main (para rodar standalone) ---
def main():
    global GROUP_ID
    
    if len(sys.argv) != 2:
        print("Uso: python3 system_monitor.py <GroupID>")
        sys.exit(1)

    GROUP_ID = sys.argv[1]

    start_monitor()  # Inicia o monitor

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n[System Monitor] Encerrado pelo utilizador.")
        client.loop_stop()
        client.disconnect()

if __name__ == "__main__":
    main()