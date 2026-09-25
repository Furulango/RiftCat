#!/usr/bin/env bash
set -euo pipefail

echo "=== RiftCat: setup de servidor ==="

# ---------------------------------------------------------------------------
# 1. Deshabilitar IPv6 (evita el hang de pip que ya nos pasó una vez)
# ---------------------------------------------------------------------------
echo "--- Deshabilitando IPv6 ---"
if ! grep -q "net.ipv6.conf.all.disable_ipv6" /etc/sysctl.conf 2>/dev/null; then
    echo "net.ipv6.conf.all.disable_ipv6 = 1" | sudo tee -a /etc/sysctl.conf > /dev/null
    echo "net.ipv6.conf.default.disable_ipv6 = 1" | sudo tee -a /etc/sysctl.conf > /dev/null
    sudo sysctl -p
else
    echo "IPv6 ya estaba deshabilitado en sysctl.conf, se omite."
fi

# ---------------------------------------------------------------------------
# 2. Instalar Docker (repo oficial, no snap)
# ---------------------------------------------------------------------------
if command -v docker &> /dev/null; then
    echo "--- Docker ya está instalado, se omite instalación ---"
else
    echo "--- Instalando Docker ---"
    sudo apt-get update
    sudo apt-get install -y ca-certificates curl gnupg

    sudo install -m 0755 -d /etc/apt/keyrings
    curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg
    sudo chmod a+r /etc/apt/keyrings/docker.gpg

    echo \
      "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu \
      $(. /etc/os-release && echo "$VERSION_CODENAME") stable" | \
      sudo tee /etc/apt/sources.list.d/docker.list > /dev/null

    sudo apt-get update
    sudo apt-get install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin

    sudo usermod -aG docker "$USER"
    echo ">>> Se agregó $USER al grupo docker. Hace falta cerrar sesión y volver a entrar (o 'newgrp docker') para que aplique sin sudo."
fi

# ---------------------------------------------------------------------------
# 3. Instalar Miniconda si no existe
# ---------------------------------------------------------------------------
if command -v conda &> /dev/null; then
    echo "--- Conda ya está instalado, se omite instalación ---"
else
    echo "--- Instalando Miniconda ---"
    curl -fsSL https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh -o /tmp/miniconda.sh
    bash /tmp/miniconda.sh -b -p "$HOME/miniconda3"
    "$HOME/miniconda3/bin/conda" init bash
    echo ">>> Miniconda instalado. Hace falta abrir una terminal nueva (o 'source ~/.bashrc') para que 'conda' esté disponible."
fi

# ---------------------------------------------------------------------------
# 4. Crear ~/riftcat-env.sh SOLO si no existe (no pisar uno que ya tengas)
# ---------------------------------------------------------------------------
ENV_FILE="$HOME/riftcat-env.sh"
if [ -f "$ENV_FILE" ]; then
    echo "--- $ENV_FILE ya existe, no se toca ---"
else
    echo "--- Creando plantilla de $ENV_FILE ---"
    cat > "$ENV_FILE" << 'EOF'
export RIOT_API_KEY="RGAPI-PON_TU_KEY_AQUI"
export AWS_ACCESS_KEY_ID="PON_TU_USUARIO_MINIO"
export AWS_SECRET_ACCESS_KEY="PON_TU_PASSWORD_MINIO"
export AWS_ENDPOINT_URL_S3="http://localhost:9000"
export PYTHON_KEYRING_BACKEND="keyring.backends.null.Keyring"
EOF
    echo ">>> Edita $ENV_FILE con tus credenciales reales antes de correr el poller (nano $ENV_FILE)."
fi

echo ""
echo "=== Setup base terminado ==="
echo "Pasos que TE FALTAN hacer a mano (fuera de este script):"
echo "  1. Cerrar sesión y volver a entrar (para que aplique el grupo docker y conda)."
echo "  2. nano ~/riftcat-env.sh   -> poner tu API key real y credenciales de MinIO."
echo "  3. Correr: bash levantar_minio.sh    (siguiente script)."
echo "  4. Correr: bash crear_entorno_python.sh"
