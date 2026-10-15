#!/bin/bash
# run_poller.sh
# Un ciclo completo de ingesta: descubrir, un worker por region en paralelo y rangos.
# Pensado para cron. Uso: ./bash_script/run_poller.sh
# Codigos de salida: 0 ok, 1 key invalida, 2 Riot caido (lo pendiente queda en cola).

python -m core.poller ciclo --limite-descubrir 100 --limite-worker 500 --limite-rangos 50
