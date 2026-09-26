#!/bin/bash
# run_poller.sh
# Corre el poller con parametros de produccion.
# Uso: ./bash_script/run_poller.sh

python -c "from core.poller import run; run(count_por_jugador=10, limite_revision=100)"
