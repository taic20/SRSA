#!/bin/bash

# Script para iniciar apenas Shelves (S1-S10) e Robots (AMR-1 a AMR-4)
# Pressione Ctrl+C para parar TUDO

GROUP_ID="GrupoTiago"

cleanup() {
    echo "=== A parar Shelves e Robots... ==="
    pkill -f "python3.*shelves.py"
    pkill -f "python3.*amr_robot.py"
    echo "=== Shelves e Robots parados ==="
    exit 0
}

trap cleanup INT

echo "=== Iniciando Shelves e Robots ==="
echo "Group ID: $GROUP_ID"
echo ""

# 1. Shelves (S1-S10)
echo "[1] Iniciando Shelves (S1 a S10)..."
for i in {1..10}; do
    python3 shelves.py $GROUP_ID S$i 10 &
done
sleep 2

# 2. Robots (AMR-1 a AMR-4)
echo "[2] Iniciando Robots (AMR-1 a AMR-4)..."
for id in AMR-1 AMR-2 AMR-3 AMR-4; do
    python3 amr_robot.py $GROUP_ID $id &
done
sleep 1

echo ""
echo "=== Shelves e Robots em execução! ==="
echo "Inicie os outros componentes manualmente."
echo "Para parar TUDO, pressione Ctrl+C neste terminal."
echo ""

wait