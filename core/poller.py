import time
from datetime import datetime

from core.riot_client import get_puuids_challenger, get_match_ids, get_match_detail
from core.db import get_conn, filtrar_nuevos, marcar_visto
from core.parquet_writer import armar_registro, guardar_parquet, subir_a_raw
from core.logger_config import get_logger

logger = get_logger(__name__)

ESPERA = 1.2
TEMP_PATH = "/tmp/riftcat_temp.parquet"


def run(count_por_jugador=5, limite_puuids=None):
    conn = get_conn()

    puuids = get_puuids_challenger()
    if limite_puuids:
        puuids = puuids[:limite_puuids]
    logger.info(f"Puuids obtenidos: {len(puuids)}")

    todos_los_ids = set()
    for puuid in puuids:
        ids = get_match_ids(puuid, count=count_por_jugador)
        todos_los_ids.update(ids)
        time.sleep(ESPERA)

    logger.info(f"Match ids unicos encontrados: {len(todos_los_ids)}")

    nuevos = filtrar_nuevos(conn, list(todos_los_ids))
    logger.info(f"Match ids nuevos por procesar: {len(nuevos)}")

    fecha = datetime.now().strftime("%Y-%m-%d")
    subidos = 0

    for i, match_id in enumerate(nuevos, start=1):
        try:
            detalle = get_match_detail(match_id)
            if detalle:
                registro = armar_registro(match_id, detalle)
                guardar_parquet(registro, TEMP_PATH)
                subir_a_raw(TEMP_PATH, match_id, fecha)
                marcar_visto(conn, match_id)
                subidos += 1
                logger.info(f"[{i}/{len(nuevos)}] Subida OK: {match_id}")
            else:
                logger.warning(f"[{i}/{len(nuevos)}] Sin detalle (404): {match_id}")
        except Exception as e:
            logger.error(f"[{i}/{len(nuevos)}] Fallo procesando {match_id}: {e}")
        time.sleep(ESPERA)

    logger.info(f"Partidas subidas a raw: {subidos}")
    conn.close()


if __name__ == "__main__":
    run(count_por_jugador=3, limite_puuids=5)