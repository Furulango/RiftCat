#!/usr/bin/env bash
set -euo pipefail

# Corre esto DESPUÉS de haber cerrado sesión y vuelto a entrar tras setup_server.sh,
# y después de haber editado ~/riftcat-env.sh con tus credenciales reales.

source "$HOME/riftcat-env.sh"

if docker ps -a --format '{{.Names}}' | grep -q '^riftcat-minio$'; then
    echo "--- Contenedor riftcat-minio ya existe, iniciando si estaba detenido ---"
    docker start riftcat-minio
else
    echo "--- Creando contenedor riftcat-minio ---"
    docker run -d \
      --name riftcat-minio \
      -p 9000:9000 -p 9001:9001 \
      -e "MINIO_ROOT_USER=${AWS_ACCESS_KEY_ID}" \
      -e "MINIO_ROOT_PASSWORD=${AWS_SECRET_ACCESS_KEY}" \
      -v "$HOME/minio-data:/data" \
      quay.io/minio/minio:RELEASE.2024-05-28T17-19-04Z server /data --console-address ":9001"
fi

echo "--- Esperando a que MinIO responda ---"
sleep 3

echo "--- Creando buckets riftcat-raw y riftcat-curated (si no existen) ---"
docker run --rm --network host \
  -e MC_HOST_local="http://${AWS_ACCESS_KEY_ID}:${AWS_SECRET_ACCESS_KEY}@localhost:9000" \
  quay.io/minio/mc:latest mb --ignore-existing local/riftcat-raw

docker run --rm --network host \
  -e MC_HOST_local="http://${AWS_ACCESS_KEY_ID}:${AWS_SECRET_ACCESS_KEY}@localhost:9000" \
  quay.io/minio/mc:latest mb --ignore-existing local/riftcat-curated

echo ""
echo "=== MinIO listo ==="
echo "Consola web: http://$(hostname -I | awk '{print $1}'):9001"
