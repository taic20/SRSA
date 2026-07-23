import paho.mqtt.client as mqtt
import sys
import time
from datetime import datetime

# --- Configurações ---
# O BROKER DEVE SER O MESMO USADO PELOS SIMULADORES (127.0.0.1 para teste local)
MQTT_BROKER = "127.0.0.1" 
MQTT_PORT = 1883
GROUP_ID = None  # Será definido pelo argumento de linha de comando

# --- Funções do MQTT ---

def on_connect(client, userdata, flags, rc):
    """Callback chamado quando a conexão é estabelecida."""
    if rc == 0:
        print(f"\n[INFO]: Conectado ao broker MQTT com sucesso (Code: {rc})")
        
        # Subscrição ampla para RAW Data e Internal Data, conforme requisitos do projeto
        topic_raw = f"warehouse/{GROUP_ID}/#"
        topic_internal = f"{GROUP_ID}/internal/#"
        
        # Subscreve a ambos os tópicos
        client.subscribe([(topic_raw, 0), (topic_internal, 0)])
        
        print(f"[INFO]: Subscrito em RAW Data: '{topic_raw}'")
        print(f"[INFO]: Subscrito em Internal Data: '{topic_internal}'")
        print("\n--- INÍCIO DO REGISTRO DE MENSAGENS ---\n")
    else:
        print(f"[ERRO]: Falha na conexão. Código de retorno: {rc}")
        sys.exit(1)

def on_message(client, userdata, msg):
    """
    Callback chamado quando uma mensagem é recebida.
    Formato de exibição exigido: [time]: [topic]: [message]
    """
    current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    topic = msg.topic
    
    try:
        # Tenta decodificar o payload como string (para JSON)
        payload_str = msg.payload.decode('utf-8')
        message_display = payload_str
        
    except UnicodeDecodeError:
        # Se falhar, é provável que seja um payload binário (Ex: comandos para o AMR)
        message_display = f"PAYLOAD BINÁRIO (HEX): {msg.payload.hex()}"
        
    # Exibe a mensagem no formato [time]: [topic]: [message]
    print(f"[{current_time}]: {topic}: {message_display}")


# --- Main ---

def main():
    global GROUP_ID
    
    if len(sys.argv) != 2:
        print("Uso: python3 mqtt_debugger.py <GroupID>")
        print("Exemplo: python3 mqtt_debugger.py MyGroup")
        sys.exit(1)

    GROUP_ID = sys.argv[1]

    # Configura o cliente MQTT
    client_id = f"debugger_{GROUP_ID}_{int(time.time())}"
    client = mqtt.Client(client_id=client_id)
    
    # Atribui os callbacks
    client.on_connect = on_connect
    client.on_message = on_message
    
    print("-" * 50)
    print(f"MQTT Debugger iniciado para o Grupo: {GROUP_ID}")
    print(f"Conectando ao Broker: {MQTT_BROKER}:{MQTT_PORT}")
    print("-" * 50)
    
    try:
        client.connect(MQTT_BROKER, MQTT_PORT, 60)
    except Exception as e:
        print(f"\n[ERRO]: Não foi possível conectar ao broker MQTT. Erro: {e}")
        sys.exit(1)
        
    # Bloqueia e processa o tráfego MQTT (substitui o loop_start() + while True)
    try:
        client.loop_forever() 
    except KeyboardInterrupt:
        print("\n[INFO]: MQTT Debugger Parado pelo usuário.")
        client.disconnect()
        sys.exit(0)

if __name__ == "__main__":
    main()