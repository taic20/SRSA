import paho.mqtt.client as mqtt
import json
import time
import sys
from datetime import datetime, timezone
import struct
import socket
import threading

from influxdb_client_3 import InfluxDBClient3, Point

# --- Configurações Gerais ---
MQTT_BROKER = "127.0.0.1" 
MQTT_PORT = 1883
UDP_ALERT_PORT = 9090  # Porta para receber alertas do System Monitor

# --- Configurações do InfluxDB Cloud ---
token = "pCUTr7dvCt32PSPpMIqspvrjMspZ1w18zsWwqVC3uhcJCxBFDKBrLaxfpks8WZ03pQYOkZRcYiKE2GMlRUUmDA==" 
org = "ficha6" 
host = "eu-central-1-1.aws.cloud2.influxdata.com" 
database = "Projeto" 

# --- Constantes de Tradução ---
CMD_EXECUTE_TASK = 0x01
CMD_FORCE_CHARGE = 0x03  # Comando de override

# Mapas INVERSOS para converter "S1" -> 0x01 e "P1" -> 0x03
SHELF_ID_MAP_INV = {
    'S1': 0x01, 'S2': 0x02, 'S3': 0x03, 'S4': 0x04, 'S5': 0x05,
    'S6': 0x06, 'S7': 0x07, 'S8': 0x08, 'S9': 0x09, 'S10': 0x0A
}
STATION_ID_MAP_INV = {'P1': 0x03, 'P2': 0x04, 'P3': 0x05}

# --- Clientes globais ---
INFLUX_CLIENT = None
MQTT_CLIENT = None
GROUP_ID = None  # Será definido no main

def initialize_influxdb():
    """Inicializa o cliente InfluxDB (Versão 3)."""
    global INFLUX_CLIENT
    try:
        INFLUX_CLIENT = InfluxDBClient3(
            host=host, 
            token=token, 
            org=org, 
            database=database
        )
        print("Conexão ao InfluxDB (V3) estabelecida com sucesso.")
    except Exception as e:
        print(f"Erro ao conectar ao InfluxDB: {e}")
        INFLUX_CLIENT = None

def publish_clean_data(group_id, topic_type, asset_id, data):
    """Publica dados limpos (JSON) para o tópico interno."""
    if topic_type == 'amr':
        internal_topic = f"{group_id}/internal/amr/{asset_id}/status"
    elif topic_type == 'locations':
        internal_topic = f"{group_id}/internal/static/{asset_id}/status"
    else:
        return
        
    MQTT_CLIENT.publish(internal_topic, json.dumps(data), qos=1)
    print(f"[GW] Reencaminhei para: {internal_topic}")

def write_to_influxdb(topic, data):
    """Escreve dados no InfluxDB e chama a publicação de dados limpos."""
    if not INFLUX_CLIENT:
        parts = topic.split('/')
        if len(parts) >= 5:
             if parts[2] == 'amr': publish_clean_data(parts[1], 'amr', data.get("robot_id"), data)
             elif parts[2] == 'locations': publish_clean_data(parts[1], 'locations', data.get("asset_id"), data)
        return

    parts = topic.split('/')
    if len(parts) < 5 or parts[0] != 'warehouse':
        return
        
    group_id = parts[1]
    timestamp_data = data.get("timestamp", datetime.now(timezone.utc))
    
    # 1. Dados do Robô (AMR)
    if parts[2] == 'amr' and parts[4] == 'status':
        try:
            point = Point("amr_status") \
                .tag("group_id", group_id) \
                .tag("robot_id", data.get("robot_id")) \
                .field("battery", float(data.get("battery"))) \
                .tag("status", data.get("status")) \
                .tag("location_id", data.get("location_id")) \
                .time(timestamp_data)
                
            INFLUX_CLIENT.write(point)
            publish_clean_data(group_id, 'amr', data.get("robot_id"), data)
            
        except Exception as e:
            print(f"Erro InfluxDB AMR: {e}")

    # 2. Dados de Sensores Estáticos (Shelf / Location)
    elif parts[2] == 'locations' and parts[5] == 'status':
        try:
            asset_id = data.get("asset_id")
        
            # NORMALIZAÇÃO: Se for SHELF e unit == "units", converte para kg
            stock_value = float(data.get("stock"))
            unit = data.get("unit", "units")
            item_id = data.get("item_id")
        
            # Conversão para kg se necessário (1 unit = 23kg)
            if data.get("type") == "SHELF" and unit == "units":
                stock_kg = stock_value * 23.0
            else:
                stock_kg = stock_value  # já está em kg
        
            point = Point("asset_status").tag("group_id", group_id).tag("zone_id", parts[3]).tag("asset_id", asset_id).tag("type", data.get("type")).field("stock_kg", stock_kg).field("stock_original", stock_value).tag("unit", unit).tag("item_id", item_id).time(timestamp_data)
            
            INFLUX_CLIENT.write(point)
            publish_clean_data(group_id, 'locations', asset_id, data)
        
        except Exception as e:
            print(f"Erro InfluxDB Locations: {e}")

# --- Servidor UDP (Alertas do System Monitor) ---
def udp_alert_listener():
    """Recebe alertas do System Monitor na porta 9090"""
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind(("0.0.0.0", UDP_ALERT_PORT))
    print(f"\n[GW UDP] Servidor de alertas UDP ouvindo na porta {UDP_ALERT_PORT}")
    
    while True:
        data, addr = sock.recvfrom(1024)
        try:
            alert = json.loads(data.decode('utf-8'))
            robot_id = alert.get('robot_id')
            level = alert.get('level')
            override_task = alert.get('override_task')
            
            print(f"\n[GW UDP] Alerta recebido de {addr}: {alert}")
            
            # Traduz e envia comando binário
            if override_task == "FORCE_CHARGE" and level == "CRITICAL":
                # Envia comando 0x03 para o robot
                payload_binary = struct.pack('>BBB', CMD_FORCE_CHARGE, 0x00, 0x00)
                command_topic = f"warehouse/{GROUP_ID}/amr/{robot_id}/command"
                MQTT_CLIENT.publish(command_topic, payload_binary, qos=1)
                print(f"[GW UDP] Comando FORCE_CHARGE (0x03) enviado para {robot_id}")
                print(f"[GW UDP] Topico: {command_topic} | Hex: {payload_binary.hex().upper()}")
                
        except Exception as e:
            print(f"[GW UDP] Erro ao processar alerta: {e}")

# --- Funções do MQTT ---
def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print(f"\nConectado ao broker MQTT (Code: {rc})")
        group_id = userdata.get('group_id')
        
        # Subscreve a RAW Data E ao tópico de Despacho do Coordinator
        topic_raw = f"warehouse/{group_id}/#"
        topic_coordinator = f"{group_id}/internal/tasks/dispatch"
        
        client.subscribe([(topic_raw, 0), (topic_coordinator, 0)])
        print(f"Subscrito: {topic_raw}")
        print(f"Subscrito: {topic_coordinator}")
    else:
        print(f"Falha na conexão. Código: {rc}")
        sys.exit(1)

def on_message(client, userdata, msg):
    topic = msg.topic
    group_id = userdata.get('group_id')
    print(f"[GW] Recebi: {topic}")
    
    # --- ROTA 1: TRADUTOR (JSON -> Binário) ---
    if topic.endswith("/internal/tasks/dispatch"):
        try:
            task_command = json.loads(msg.payload.decode('utf-8'))
            robot_id = task_command.get('robot_id')
            shelf_str = task_command.get('shelf_id')
            station_str = task_command.get('station_id')
            
            print(f"\n[TRADUTOR] Recebido despacho: {robot_id} -> {shelf_str} -> {station_str}")
            
            shelf_byte = SHELF_ID_MAP_INV.get(shelf_str)
            station_byte = STATION_ID_MAP_INV.get(station_str)
            
            if shelf_byte is None or station_byte is None:
                print(f"[ERRO] IDs desconhecidos: {shelf_str}, {station_str}")
                return

            payload_binary = struct.pack('>BBB', CMD_EXECUTE_TASK, shelf_byte, station_byte)
            command_topic = f"warehouse/{group_id}/amr/{robot_id}/command"
            client.publish(command_topic, payload_binary, qos=1)
            print(f"[GW] Comando BINÁRIO enviado para {robot_id}: {payload_binary.hex().upper()}")
            
        except Exception as e:
            print(f"Erro ao processar despacho: {e}")
        return

    # --- ROTA 2: PROCESSAMENTO DE DADOS RAW ---
    if f"{group_id}/internal" in topic or topic.endswith("/command"):
        return

    try:
        payload_data = json.loads(msg.payload.decode('utf-8'))
        write_to_influxdb(topic, payload_data)
        print(f"[GW] Processado e reencaminhado: {topic}")
    except json.JSONDecodeError:
        pass
    except Exception as e:
        print(f"Erro msg raw: {e}")

# --- Main ---
def main():
    if len(sys.argv) != 2:
        print("Uso: python3 warehouse_gateway.py <GroupID>")
        sys.exit(1)

    group_id = sys.argv[1]
    global GROUP_ID
    GROUP_ID = group_id

    print("--- Warehouse Gateway Iniciado ---")
    initialize_influxdb()
    
    global MQTT_CLIENT
    client_id = f"gateway_{group_id}_{int(time.time())}"
    MQTT_CLIENT = mqtt.Client(
        client_id=client_id,
        userdata={'group_id': group_id}
    )
    MQTT_CLIENT.on_connect = on_connect
    MQTT_CLIENT.on_message = on_message
    
    # Inicia o servidor UDP de alertas
    udp_thread = threading.Thread(target=udp_alert_listener, daemon=True)
    udp_thread.start()
    
    try:
        MQTT_CLIENT.connect(MQTT_BROKER, MQTT_PORT, 60)
        MQTT_CLIENT.loop_forever()
    except KeyboardInterrupt:
        print("\nGateway a encerrar.")
    except Exception as e:
        print(f"Erro fatal: {e}")

if __name__ == "__main__":
    main()