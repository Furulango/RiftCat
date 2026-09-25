#!/usr/bin/env bash
set -euo pipefail

# Corre esto DESPUÉS de levantar_minio.sh, parado dentro de la carpeta del repo clonado.

ENV_NAME="riftcat"

source "$HOME/miniconda3/etc/profile.d/conda.sh"

if conda env list | grep -q "^${ENV_NAME} "; then
    echo "--- El entorno conda '${ENV_NAME}' ya existe, se omite creación ---"
else
    echo "--- Creando entorno conda '${ENV_NAME}' ---"
    conda create -n "${ENV_NAME}" python=3.11 -y
fi

conda activate "${ENV_NAME}"

if [ ! -f "requirements.txt" ]; then
    echo "ERROR: no se encontró requirements.txt en $(pwd). ¿Estás dentro de la carpeta del repo?"
    exit 1
fi

echo "--- Instalando dependencias ---"
pip install -r requirements.txt --break-system-packages

echo ""
echo "=== Entorno listo ==="
echo "Para usarlo en el futuro:"
echo "  source ~/riftcat-env.sh"
echo "  conda activate ${ENV_NAME}"
echo "  python -m core.poller"
