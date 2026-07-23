import socket
import threading
import time
import sys
import json
import paho.mqtt.client as mqtt
from queue import Queue
from datetime import datetime

# --- Configurações ---
MQTT_BROKER = "127.0.0.1"
MQTT_PORT = 1883
UDP_HOST = "0.0.0.0"
UDP_PORT = 5005
GROUP_ID = None

# --- Variáveis Globais (Estado do Sistema) ---
mqtt_client = None
inventory = {}  # { 'item_id': {'shelf_id': 'S1', 'stock': 10} }
robots = {}     # { 'AMR-1': {'status': 'IDLE', 'battery': 100} }
order_queue = Queue()
system_monitor = None  # Será definido no main

# --- Funções Auxiliares ---
def datetime_now_iso():
    return datetime.now().isoformat(timespec='milliseconds') + 'Z'

# --- Funções MQTT ---
def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print(f"[MQTT] Conectado ao Broker {MQTT_BROKER}")
        topic = f"{GROUP_ID}/internal/#"
        client.subscribe(topic)
        print(f"[MQTT] Subscrito em: {topic}")
    else:
        print(f"[ERRO] Falha ao conectar MQTT: {rc}")

def on_message(client, userdata, msg):
    global inventory, robots
    try:
        topic = msg.topic
        payload = json.loads(msg.payload.decode('utf-8'))

        # Atualizar Inventário (shelves)
        if "static" in topic and "status" in topic:
            shelf_id = payload.get("asset_id")
            item_id = payload.get("item_id")
            stock = payload.get("stock", 0)
            if item_id:
                inventory[str(item_id)] = {'shelf_id': shelf_id, 'stock': stock}

        # Atualizar Robôs
        elif "amr" in topic and "status" in topic:
            robot_id = payload.get("robot_id")
            status = payload.get("status")
            battery = payload.get("battery")
            robots[robot_id] = {'status': status, 'battery': battery}

    except Exception as e:
        print(f"[ERRO] Processamento MQTT: {e}")

# --- Funções UDP ---
def udp_listener():
    """Ouve pedidos JSON na porta UDP 5005"""
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind((UDP_HOST, UDP_PORT))
    print(f"[UDP] Servidor ouvindo na porta {UDP_PORT}")

    while True:
        data, addr = sock.recvfrom(1024)
        try:
            json_str = data.decode('utf-8')
            print(f"\n[UDP] Pedido Recebido: {json_str}")

            order_json = json.loads(json_str)
            order = {
                'item': order_json['item'],
                'quantity': int(order_json['quantity']),
                'station': order_json['station'],
                'timestamp': time.time()
            }

            order_queue.put(order)
            print(f"[FILA] Encomenda adicionada. Pendentes: {order_queue.qsize()}")

        except json.JSONDecodeError as e:
            print(f"[ERRO] JSON Inválido ou Erro de Leitura: {e}")
        except Exception as e:
            print(f"[ERRO] UDP: {e}")

# --- Lógica de Despacho ---
def process_orders():
    """Verifica se há encomendas e robôs disponíveis"""
    if order_queue.empty():
        return

    current_order = order_queue.queue[0]
    item_needed = current_order['item']
    target_station = current_order['station']

    # 1. Verificar Parking Station
    # Consulta o System Monitor via módulo importado
    try:
        if system_monitor.get_parking_status(target_station) != "AVAILABLE":
            print(f"[COORD] Parking {target_station} está BLOCKED. Encomenda em espera.")
            return  # Mantém na fila
    except Exception as e:
        print(f"[COORD] Erro ao verificar parking: {e}. Prosseguindo...")
    
    # 2. Verificar Stock
    item_info = inventory.get(item_needed)
    if not item_info or item_info['stock'] < 1:
        return  # Aguardar stock

    target_shelf = item_info['shelf_id']

    # 3. Procurar Robô IDLE
    selected_robot = None
    for r_id, r_data in robots.items():
        if r_data['status'] == "IDLE":
            selected_robot = r_id
            break

    if selected_robot:
        order = order_queue.get()
        print(f" >>> DESPACHANDO TAREFA: {selected_robot} vai buscar Item {item_needed} em {target_shelf} → {target_station}")

        dispatch_payload = {
            "robot_id": selected_robot,
            "shelf_id": target_shelf,
            "station_id": target_station,
            "timestamp": datetime_now_iso()
        }

        topic_dispatch = f"{GROUP_ID}/internal/tasks/dispatch"
        mqtt_client.publish(topic_dispatch, json.dumps(dispatch_payload), qos=1)
        robots[selected_robot]['status'] = "ASSIGNED"
    else:
        print("[COORD] Nenhum robot IDLE disponível")

# --- Main ---
def main():
    global mqtt_client, GROUP_ID, system_monitor
    
    if len(sys.argv) != 2:
        print("ERRO: O GroupID é obrigatório.")
        print("Uso: python3 fleet_coordinator.py <GroupID>")
        sys.exit(1)

    GROUP_ID = sys.argv[1]

    # Importa e inicializa o System Monitor
    import system_monitor as sm
    sm.GROUP_ID = GROUP_ID
    sm.MQTT_BROKER = MQTT_BROKER
    sm.MQTT_PORT = MQTT_PORT
    sm.UDP_TARGET_IP = "127.0.0.1"
    sm.UDP_TARGET_PORT = 9090
    sm.MAIN_PROCESS = False  # Flag para não correr o loop principal
    system_monitor = sm
    
    # Inicia o MQTT do System Monitor numa thread separada
    monitor_thread = threading.Thread(target=sm.start_monitor, daemon=True)
    monitor_thread.start()
    
    time.sleep(1)  # Dá tempo ao System Monitor de se conectar

    # Configura MQTT do Coordinator
    mqtt_client = mqtt.Client(client_id=f"fleet_coord_{GROUP_ID}")
    mqtt_client.on_connect = on_connect
    mqtt_client.on_message = on_message
    
    try:
        mqtt_client.connect(MQTT_BROKER, MQTT_PORT, 60)
        mqtt_client.loop_start()
    except Exception as e:
        print(f"Erro ao conectar MQTT: {e}")
        sys.exit(1)

    # Inicia UDP em thread separada
    udp_thread = threading.Thread(target=udp_listener, daemon=True)
    udp_thread.start()

    # Loop principal de decisão
    try:
        while True:
            process_orders()
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nFleet Coordinator a encerrar...")
        mqtt_client.loop_stop()

if __name__ == "__main__":
    main()