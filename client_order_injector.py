import socket
import time
import random
import json
import sys

# --- Configurações ---
# O IP deve ser onde o fleet_coordinator.py está a correr
TARGET_IP = "127.0.0.1"
TARGET_PORT = 5005

# --- Configuração da Simulação ---
# IMPORTANTE: Estes IDs de items TÊM de existir nas tuas prateleiras (shelves.py)
# Se pedires um item que nenhuma prateleira tem, o Coordinator vai ignorar.
POSSIBLE_ITEMS = ["item_A","item_B","item_C","item_D","item_E","item_F","item_G","item_H","item_I","item_J"] 

# Estações de destino (Picking ou Packing)
POSSIBLE_STATIONS = ["P1", "P2", "P3"]

# Intervalo entre encomendas (em segundos)
MIN_DELAY = 5
MAX_DELAY = 6

ITEM_QUANTITY_RANGES = {
    # Storage-a: unidades pequenas
    "item_A": (1, 10), "item_B": (1, 10), "item_C": (1, 10),
    "item_D": (1, 10), "item_E": (1, 10),
    # Storage-b: kg maiores
    "item_F": (10, 100), "item_G": (10, 100), "item_H": (10, 100),
    "item_I": (10, 100), "item_J": (10, 100)
}

def main():
    print(f"--- Client Order Injector (Automático) ---")
    print(f"Alvo UDP: {TARGET_IP}:{TARGET_PORT}")
    print(f"Itens Possíveis: {POSSIBLE_ITEMS}")
    print("Iniciando geração de encomendas... (Pressione CTRL+C para parar)")
    
    # Cria o socket UDP
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    
    try:
        while True:
            # 1. Escolher dados aleatórios
            item_id = random.choice(POSSIBLE_ITEMS)
            station_id = random.choice(POSSIBLE_STATIONS)
            min_q, max_q = ITEM_QUANTITY_RANGES.get(item_id, (1,5))
            quantity = random.randint(min_q, max_q)
            
            # 2. Construir XML
            order = {
                'item': item_id,
                'quantity': quantity,
                'station': station_id,
                'timestamp': time.time()
            }
            
            # 3. Enviar via UDP
            try:
                message = json.dumps(order).encode('utf-8')
                sock.sendto(message, (TARGET_IP, TARGET_PORT))
                print(f"[ENCOMENDA ENVIADA] Item: {item_id}, Quantidade: {quantity}, Estação: {station_id}")
            except Exception as e:
                print(f"[ERRO] Falha ao enviar UDP: {e}")
            
            # 4. Aguardar tempo aleatório antes da próxima
            delay = random.randint(MIN_DELAY, MAX_DELAY)
            print(f"   ... aguardando {delay} segundos ...")
            time.sleep(delay)
            
    except KeyboardInterrupt:
        print("\nInjector parado pelo utilizador.")
    finally:
        sock.close()

if __name__ == "__main__":
    main()