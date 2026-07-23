import paho.mqtt.client as mqtt
import json
import time
import sys
import struct
import random
from datetime import datetime

# --- Configurações ---
MQTT_BROKER = "127.0.0.1"
MQTT_PORT = 1883
BATTERY_DECAY_RATE = 1.0  # 1% por segundo ativo 
CHARGING_DURATION = 10  # 10 segundos para carregar 
STALL_PROBABILITY = 0.05 # 5% de chance de STALLED durante MOVING 
LOW_BATTERY_THRESHOLD = 15.0  # 15% de bateria

# Constantes de Comando Binário (Tabela 2) 
COMMAND_EXECUTE_TASK = 0x01
COMMAND_FORCE_CHARGE = 0x03

# Mapeamento de IDs
SHELF_ID_MAP = {
    0x01: 'S1', 0x02: 'S2', 0x03: 'S3', 0x04: 'S4', 0x05: 'S5',
    0x06: 'S6', 0x07: 'S7', 0x08: 'S8', 0x09: 'S9', 0x0A: 'S10'  
}
STATION_ID_MAP = {0x03: 'P1', 0x04: 'P2', 0x05: 'P3'}

# Dicionário de Ciclo de Execução
EXECUTION_CYCLE = {
    "MOVING_TO_PICK": 3.0,  # 3 segundos
    "PICKING": 1.0,         # 1 segundo
    "MOVING_TO_DROP": 2.0,  # 2 segundos
    "DROPPING": 1.0         # 1 segundo
}
ACTIVE_STATES = list(EXECUTION_CYCLE.keys()) + ["MOVING_TO_CHARGE"]

# --- Estado do Robô ---
robot_state = {}


# --- Funções do MQTT ---

def on_connect(client, userdata, flags, rc):
    """Callback chamado quando a conexão com o broker é estabelecida."""
    if rc == 0:
        print(f"Conectado ao broker MQTT com sucesso (Code: {rc})")
        command_topic = f"warehouse/{robot_state['group_id']}/amr/{robot_state['robot_id']}/command"
        client.subscribe(command_topic, qos=1)
        print(f"Subscrito em: {command_topic}")
    else:
        print(f"Falha na conexão. Código de retorno: {rc}")

def on_message(client, userdata, msg):
    """Callback chamado quando uma mensagem é recebida no tópico subscrito."""
    payload = msg.payload
    global robot_state
    topic = msg.topic

    if topic.endswith("/command"):
        if len(payload) != 3:
            print(f"Payload de comando de tamanho inválido ({len(payload)} bytes). Esperado 3 bytes.")
            return
            
        try:
            # Desempacotar o payload binário: 1 byte (Tipo de Comando) + 2 bytes (IDs)
            command_type, shelf_id_byte, station_id_byte = struct.unpack('>BBB', payload)
            
            # Mapeamento de IDs
            target_shelf = SHELF_ID_MAP.get(shelf_id_byte, f'S{shelf_id_byte}')
            target_station = STATION_ID_MAP.get(station_id_byte, f'P{station_id_byte}')

            print(f"\n>>> COMANDO RECEBIDO <<<")
            print(f"Tipo: 0x{command_type:02X}, Prateleira: {target_shelf}, Estação: {target_station}")
            print(f"Status atual: {robot_state['status']}")
            
            # 1. Comando de Execução de Tarefa (EXECUTE_TASK)
            if command_type == COMMAND_EXECUTE_TASK:
                if robot_state['status'] == "IDLE":
                    robot_state['target_shelf'] = target_shelf
                    robot_state['target_station'] = target_station
                    
                    print(f"✓ Tarefa Aceite: PICK {target_shelf}, DROP {target_station}")
                    start_state_transition("MOVING_TO_PICK")
                else:
                    print(f"✗ Ignorado: Robô não está IDLE (Status: {robot_state['status']})")
            
            # 2. Comando de Forçar Carregamento (FORCE_CHARGE)
            elif command_type == COMMAND_FORCE_CHARGE:
                # ACEITA EM QUALQUER ESTADO (incluindo STALLED, MOVING, PICKING, etc.)
                print(f"\n>>> COMANDO RECEBIDO <<<")
                print(f"Tipo: 0x{command_type:02X} (FORCE_CHARGE OVERRIDE)")
                print(f"Status atual: {robot_state['status']}")
                print(f"✓ Comando de EMERGÊNCIA aceite - Iniciando MOVING_TO_CHARGE")
                
                # Força transição imediata para MOVING_TO_CHARGE
                robot_state['status'] = "MOVING_TO_CHARGE"
                robot_state['state_start_time'] = time.time()
                robot_state['state_duration'] = 0.0
                publish_status(client)  # Publica imediatamente o novo estado
                start_state_transition("MOVING_TO_CHARGE")
                
            else:
                print(f"✗ Comando Binário Desconhecido: 0x{command_type:02X}")

        except struct.error as e:
            print(f"Erro ao desempacotar payload binário: {e}")
        except Exception as e:
            print(f"Erro inesperado ao processar comando: {e}")

def publish_status(client):
    """Constrói e publica a mensagem de status do AMR."""
    topic = f"warehouse/{robot_state['group_id']}/amr/{robot_state['robot_id']}/status"
    
    # Mapeamento Location ID
    location_id_map = {
        "IDLE": "DOCK",
        "MOVING_TO_PICK": "TRANSIT",
        "PICKING": robot_state['target_shelf'],
        "MOVING_TO_DROP": "TRANSIT",
        "DROPPING": "PACKING_ZONE",
        "MOVING_TO_CHARGE": "TRANSIT",
        "CHARGING": "CHARGING_STATION",
        "STALLED": robot_state['location_id']  # Mantém a última localização
    }
    
    robot_state['location_id'] = location_id_map.get(robot_state['status'], "UNKNOWN")
    
    # Estrutura JSON dos Dados do AMR
    payload = {
        "robot_id": robot_state['robot_id'],
        "timestamp": datetime.now().isoformat(timespec='milliseconds') + 'Z',
        "location_id": robot_state['location_id'],
        "battery": round(robot_state['battery']),
        "status": robot_state['status']
    }
    
    client.publish(topic, json.dumps(payload), qos=1)

# --- Lógica de Estado e Timer ---

def start_state_transition(new_status):
    """Muda o estado do robô e redefine o temporizador."""
    global robot_state
    
    print(f"\n<<< TRANSIÇÃO DE ESTADO >>>")
    print(f"[{robot_state['robot_id']}]: {robot_state['status']} -> {new_status}")
    print(f"Bateria: {robot_state['battery']:.1f}%")
    print("----------------------------")
    
    robot_state['status'] = new_status
    robot_state['state_start_time'] = time.time()
    
    # Define a duração do novo estado
    if new_status in EXECUTION_CYCLE:
        robot_state['state_duration'] = EXECUTION_CYCLE[new_status]
    elif new_status == "CHARGING":
        robot_state['state_duration'] = CHARGING_DURATION
    elif new_status == "IDLE":
        robot_state['state_duration'] = 2 
    else:
        robot_state['state_duration'] = 0.0

def update_state_and_battery(client, delta_time):
    """Atualiza o estado do robô e os níveis de bateria com base no tempo."""
    
    # CRÍTICO: Se estiver STALLED, não processa timers nem bateria
    # Mas PERMITE receber comandos de override (tratados no on_message)
    if robot_state['status'] == "STALLED":
        return
        
    status = robot_state['status']
    
    # 1. Bateria e Carga
    if status in ACTIVE_STATES:
        robot_state['battery'] = max(0.0, robot_state['battery'] - (BATTERY_DECAY_RATE * delta_time))
        
        if robot_state['battery'] <= LOW_BATTERY_THRESHOLD and status not in ["MOVING_TO_CHARGE", "CHARGING","STALLED"]:
            print(f"!!! BATERIA CRÍTICA ({robot_state['battery']:.1f}%). FORÇANDO MOVING_TO_CHARGE. !!!")
            start_state_transition("MOVING_TO_CHARGE")
            return
            
    elif status == "CHARGING":
        pass
        
    # 2. Lógica de Falha (STALLED)
    if status.startswith("MOVING"):
        if random.random() < (STALL_PROBABILITY * delta_time): 
            print("!!! FALHA DETECTADA: ENTRANDO NO ESTADO STALLED !!!")
            robot_state['status'] = "STALLED"
            return

    # 3. Lógica de Transição de Estado (Timers)
    if status in EXECUTION_CYCLE or status == "CHARGING" or status == "MOVING_TO_CHARGE":
        elapsed = time.time() - robot_state['state_start_time']
        
        if elapsed >= robot_state['state_duration']:
            if status == "MOVING_TO_PICK":
                robot_state['location_id'] = robot_state['target_shelf']
                start_state_transition("PICKING")
                
            elif status == "PICKING":
                start_state_transition("MOVING_TO_DROP")

            elif status == "MOVING_TO_DROP":
                start_state_transition("DROPPING")

            elif status == "DROPPING":
                robot_state['location_id'] = "PACKING_ZONE"

                # Notificar que o stock foi retirado
                stock_removed_topic = f"{robot_state['group_id']}/internal/stock/removed"
                removal_data = {
                    "asset_id": robot_state['target_shelf'],
                    "robot_id": robot_state['robot_id'],
                    "timestamp": datetime.now().isoformat(timespec='milliseconds') + 'Z'
                }
                client.publish(stock_removed_topic, json.dumps(removal_data), qos=1)
                print(f"-> Stock retirado da prateleira {robot_state['target_shelf']}")
                
                start_state_transition("IDLE")
                
            elif status == "MOVING_TO_CHARGE":
                start_state_transition("CHARGING")

            elif status == "CHARGING":
                robot_state['battery'] = 100.0
                print("--- CARGA COMPLETA. Retornando a IDLE. ---")
                start_state_transition("IDLE")
                
# --- Main ---

def initialize_robot(group_id, robot_id):
    """Inicializa o estado inicial do robô."""
    global robot_state
    robot_state = {
        "group_id": group_id,
        "robot_id": robot_id,
        "battery": 100.0,
        "status": "IDLE",
        "location_id": "DOCK",
        "state_start_time": time.time(),
        "state_duration": None,
        "target_shelf": None,
        "target_station": None,
        "last_update_time": time.time()
    }
    
def main():
    if len(sys.argv) != 3:
        print("Uso: python3 amr_robot.py <GroupID> <robot_id>")
        print("Exemplo: python3 amr_robot.py MyGroup AMR-1")
        sys.exit(1)

    group_id = sys.argv[1]
    robot_id = sys.argv[2].upper() 

    initialize_robot(group_id, robot_id)

    client_id = f"amr_{robot_id}_{group_id}"
    client = mqtt.Client(client_id=client_id)
    client.on_connect = on_connect
    client.on_message = on_message
    
    print(f"\n{'='*50}")
    print(f"Robô {robot_id} (Grupo {group_id}) iniciado.")
    print(f"Status: {robot_state['status']} | Localização: {robot_state['location_id']}")
    print(f"Bateria: {robot_state['battery']}%")
    print(f"{'='*50}\n")
    
    try:
        client.connect(MQTT_BROKER, MQTT_PORT, 60)
    except Exception as e:
        print(f"Não foi possível conectar ao broker MQTT. Erro: {e}")
        sys.exit(1)
        
    client.loop_start()

    try:
        while True:
            current_time = time.time()
            delta_time = current_time - robot_state['last_update_time']
            
            update_state_and_battery(client, delta_time)
            publish_status(client)
            
            robot_state['last_update_time'] = current_time
            time.sleep(1)
            
    except KeyboardInterrupt:
        print(f"\nSimulação do Robô {robot_id} Parada.")
    finally:
        client.loop_stop()
        client.disconnect()
        print("Desconectado do MQTT.")

if __name__ == "__main__":
    main()