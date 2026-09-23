import time
from datetime import datetime

from core.riot_client import get_puuids_challenger, get_match_ids, get_match_detail
from core.db import get_conn, filtrar_nuevos, marcar_visto
from core.parquet_writer import armar_registro, guardar_parquet, subir_a_raw


ESPERA = 1.2
TEMP_PATH = "/tmp/riftcat_temp.parquet"


def run(count_por_jugador=5, limite_puuids=None):
    conn = get_conn()

    puuids = get_puuids_challenger()
    if limite_puuids:
        puuids = puuids[:limite_puuids]
    print(f"Puuids obtenidos: {len(puuids)}")

    todos_los_ids = set()
    for puuid in puuids:
        ids = get_match_ids(puuid, count=count_por_jugador)
        todos_los_ids.update(ids)
        time.sleep(ESPERA)

    print(f"Match ids unicos encontrados: {len(todos_los_ids)}")

    nuevos = filtrar_nuevos(conn, list(todos_los_ids))
    print(f"Match ids nuevos por procesar: {len(nuevos)}")

    fecha = datetime.now().date()
    subidos = 0
    detalles = []

    for match_id in nuevos:
        detalle = get_match_detail(match_id)
        if detalle:
            registro = armar_registro(match_id, detalle)
            guardar_parquet(registro, TEMP_PATH)
            subir_a_raw(TEMP_PATH, match_id, datetime.now().date())
            marcar_visto(conn, match_id)
            subidos += 1

        time.sleep(ESPERA)

    print(f"Partidas subidas a raw: {subidos}")
    conn.close()

if __name__ == "__main__":
    run(count_por_jugador=3, limite_puuids=5)