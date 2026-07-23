import paho.mqtt.client as mqtt
import json
import time
import sys
import random
from datetime import datetime

# --- Configurações ---
MQTT_BROKER = "127.0.0.1"
MQTT_PORT = 1883
REFILL_DURATION = 2  # Segundos para reabastecimento automático

# Mapeamento DETALHADO das prateleiras conforme a Tabela 1
SHELF_CONFIG = {
    # Zona storage-a: S1 a S5 (Unidade: units | 1 unit = 23 kg)
    "S1": {"zone_id": "storage-a", "item_id": "item_A", "unit": "units", "re_stock_value": 150},
    "S2": {"zone_id": "storage-a", "item_id": "item_B", "unit": "units", "re_stock_value": 120},
    "S3": {"zone_id": "storage-a", "item_id": "item_C", "unit": "units", "re_stock_value": 100},
    "S4": {"zone_id": "storage-a", "item_id": "item_D", "unit": "units", "re_stock_value": 130},
    "S5": {"zone_id": "storage-a", "item_id": "item_E", "unit": "units", "re_stock_value": 110},
    
    # Zona storage-b: S6 a S10 (Unidade: kg)
    "S6": {"zone_id": "storage-b", "item_id": "item_F", "unit": "kg", "re_stock_value": 300},
    "S7": {"zone_id": "storage-b", "item_id": "item_G", "unit": "kg", "re_stock_value": 350},
    "S8": {"zone_id": "storage-b", "item_id": "item_H", "unit": "kg", "re_stock_value": 250},
    "S9": {"zone_id": "storage-b", "item_id": "item_I", "unit": "kg", "re_stock_value": 280},
    "S10": {"zone_id": "storage-b", "item_id": "item_J", "unit": "kg", "re_stock_value": 320},
}

# --- Inicialização ---

def initialize_shelf(group_id, asset_id, update_time):
    """Inicializa as variáveis de estado da prateleira a partir do mapeamento."""
    if asset_id not in SHELF_CONFIG:
        print(f"Erro: ID de Ativo '{asset_id}' não está no mapeamento (deve ser S1-S10).")
        sys.exit(1)
        
    config = SHELF_CONFIG[asset_id]
    
    # Gerar um stock inicial aleatório (entre 70% e 100% do valor de reabastecimento)
    initial_stock = random.randint(int(config["re_stock_value"] * 0.7), config["re_stock_value"])
    
    return {
        "group_id": group_id,
        "zone_id": config["zone_id"],
        "asset_id": asset_id,
        "item_id": config["item_id"],
        "type": "SHELF",
        "stock": initial_stock,
        "unit": config["unit"],
        "update_time": update_time,
        "is_refilling": False,
        "refill_time": 0,
        "re_stock_value": config["re_stock_value"]
    }

# --- Variável global para armazenar o estado da shelf ---
# Será inicializada no main()
shelf_state = None

# --- Funções do MQTT ---

def on_connect(client, userdata, flags, rc):
    """Callback chamado quando a conexão com o broker é estabelecida."""
    if rc == 0:
        print(f"Conectado ao broker MQTT com sucesso (Code: {rc})")
        # Subscrição ao tópico interno de retirada de stock
        stock_removed_topic = f"{userdata['group_id']}/internal/stock/removed"
        client.subscribe(stock_removed_topic, qos=1)
        print(f"Subscrito em: {stock_removed_topic}")
    else:
        print(f"Falha na conexão. Código de retorno: {rc}")

def on_message(client, userdata, msg):
    """Callback chamado quando uma mensagem é recebida (escuta a retirada de stock)."""
    topic = msg.topic
    global shelf_state  # <--- IMPORTANTE: permite aceder à variável global
    
    # Debug: mostra todas as mensagens recebidas
    print(f"[SHELF] Recebi mensagem no tópico: {topic}")
    
    if topic.endswith("/internal/stock/removed"):
        try:
            removal_data = json.loads(msg.payload.decode('utf-8'))
            target_asset_id = removal_data.get("asset_id")
            robot_id = removal_data.get("robot_id")
            
            print(f"[SHELF] Mensagem de remoção: {removal_data}")
            print(f"[SHELF] Meu asset_id: {userdata['asset_id']}")
            
            # Verificar se a notificação é para ESTA prateleira
            if target_asset_id == userdata['asset_id']:
                print(f"[SHELF] ✓ Mensagem é para mim! Vou processar...")
                print(f"[SHELF] Stock antes: {shelf_state['stock']} {shelf_state['unit']}")
                
                # Verifica se a prateleira não está no meio do reabastecimento
                if not shelf_state['is_refilling']:
                    # Usa a função de redução de stock
                    decrease = calculate_stock_decrease(shelf_state['zone_id'])
                    
                    if shelf_state['stock'] > 0:
                        shelf_state['stock'] = max(0, shelf_state['stock'] - decrease)
                        print(f"\n<<< AÇÃO ROBÔ {robot_id} >>>")
                        print(f"[{target_asset_id}] Estoque reduzido em {decrease} {shelf_state['unit']}. Novo estoque: {shelf_state['stock']} {shelf_state['unit']}")
                        
                        # Se o stock chegou a zero
                        if shelf_state['stock'] == 0:
                            print(f"!!! {shelf_state['asset_id']} ESTOQUE ZERO. Iniciando reabastecimento em {REFILL_DURATION}s. !!!")
                            shelf_state['is_refilling'] = True
                            shelf_state['refill_time'] = time.time() + REFILL_DURATION
                        
                        # Forçar publicação imediata
                        publish_status(client, shelf_state)
                        
        except json.JSONDecodeError:
            print("Erro ao decodificar a notificação de retirada de stock.")
        except Exception as e:
            print(f"[SHELF] Erro ao processar mensagem de retirada de stock: {e}")

def publish_status(client, shelf_state):
    """Constrói e publica a mensagem de status da prateleira."""
    # Tópico: warehouse/{GroupID}/locations/{zone_id}/{asset_id}/status
    topic = f"warehouse/{shelf_state['group_id']}/locations/{shelf_state['zone_id']}/{shelf_state['asset_id']}/status"
    
    # Estrutura JSON dos Dados do Sensor Estático
    payload = {
        "asset_id": shelf_state['asset_id'],
        "type": shelf_state['type'],
        "item_id": shelf_state['item_id'],
        "stock": shelf_state['stock'],
        "unit": shelf_state['unit'],
        "timestamp": datetime.now().isoformat(timespec='milliseconds') + 'Z' # Adiciona Z para formato ISO 8601 completo
    }
    
    # Publicar a mensagem
    client.publish(topic, json.dumps(payload), qos=1) # Usando QoS 1 para entrega garantida
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Publicado em {topic}: {payload}")

# --- Lógica de Simulação ---

def calculate_stock_decrease(zone_id):
    """Calcula uma redução de estoque razoável com base na zona/unidade."""
    if zone_id == "storage-a":
        # Unidades (units): Redução pequena, e.g., 1 a 5 unidades por retirada
        return random.randint(1, 5)
    elif zone_id == "storage-b":
        # Quilogramas (kg): Redução maior, e.g., 10 a 30 kg por retirada
        return random.randint(10, 30)
    return 0

def simulate_shelf_logic(shelf_state):
    """Simula o reabastecimento automático (a retirada é feita pelo AMR)."""
    
    # 1. Lógica de Reabastecimento
    if shelf_state['is_refilling']:
        if time.time() >= shelf_state['refill_time']:
            # Reabastecimento concluído
            shelf_state['stock'] = shelf_state['re_stock_value']
            shelf_state['is_refilling'] = False
            print(f"--- {shelf_state['asset_id']} REABASTECIDO para {shelf_state['stock']} {shelf_state['unit']} ---")
            return True # Retorna True para publicar o status atualizado imediatamente
        return False
    
    # Não há mais lógica de retirada aqui.
    return False

# --- Main ---

def main():
    # Uso: $>python3 shelves.py {GroupID} {asset_id} {update_time}
    # O script usa o asset_id para buscar o zone_id e item_id 
    if len(sys.argv) != 4:
        print("Uso: python3 shelves.py <GroupID> <asset_id> <update_time_seconds>")
        print("Exemplo: python3 shelves.py MyGroup S1 10")
        sys.exit(1)

    group_id = sys.argv[1]
    asset_id = sys.argv[2].upper() # Garante que o ID esteja em maiúsculas
    try:
        update_time = int(sys.argv[3])
    except ValueError:
        print("Erro: <update_time_seconds> deve ser um número inteiro.")
        sys.exit(1)

    # Inicializar o estado da prateleira (valida o asset_id dentro da função)
    global shelf_state  # <--- IMPORTANTE: declarar como global
    shelf_state = initialize_shelf(group_id, asset_id, update_time)

    # Configurar o cliente MQTT
    client_id = f"shelf_{asset_id}_{group_id}"
    client = mqtt.Client(client_id=client_id, userdata={'group_id': group_id, 'asset_id': asset_id})
    client.on_connect = on_connect
    client.on_message = on_message
    print(f"Tentando conectar ao broker em {MQTT_BROKER}:{MQTT_PORT}...")
    try:
        # Conectar ao broker
        client.connect(MQTT_BROKER, MQTT_PORT, 60)
    except Exception as e:
        print(f"Não foi possível conectar ao broker MQTT. Verifique a conexão. Erro: {e}")
        sys.exit(1)
        
    client.loop_start() # Inicia um thread para o manuseio em segundo plano do MQTT

    last_publish_time = time.time()
    
    print(f"--- Simulação da Prateleira {asset_id} ({shelf_state['item_id']}) Iniciada ---")
    print(f"Zona: {shelf_state['zone_id']}, Unidade: {shelf_state['unit']}")
    print(f"Publicando a cada {update_time}s")
    
    try:
        # Publica o status inicial
        publish_status(client, shelf_state) 
        
        while True:
            # Lógica de Simulação: verifica se houve mudança de estado (e.g., retirada, reabastecimento)
            state_changed = simulate_shelf_logic(shelf_state)
            
            current_time = time.time()
            
            # Publica o status se o tempo de atualização expirou OU se o estado mudou
            if state_changed or (current_time - last_publish_time) >= update_time:
                publish_status(client, shelf_state)
                last_publish_time = current_time
            
            time.sleep(1) # Loop para permitir verificação de estado e timers

    except KeyboardInterrupt:
        print("\nSimulação de Prateleira Parada.")
    finally:
        client.loop_stop()
        client.disconnect()
        print("Desconectado do MQTT.")

if __name__ == "__main__":
    main()