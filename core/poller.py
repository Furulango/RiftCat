import time
from datetime import datetime

from core.riot_client import get_puuids_challenger, get_match_ids, get_match_detail, get_rank_by_puuid, get_puuids_by_tier
from core.db import (
    get_conn, filtrar_nuevos, marcar_visto, guardar_puuid,
    descubrir_puuids, obtener_puuids_para_revisar, marcar_puuid_revisado
)
from core.parquet_writer import armar_registro, guardar_parquet, subir_a_raw
from core.logger_config import get_logger

logger = get_logger(__name__)

ESPERA = 1.2
TEMP_PATH = "/tmp/riftcat_temp.parquet"

SERVIDORES = [
    "br1", "eun1", "euw1", "jp1", "kr",
    "la1", "la2", "me1", "na1", "oc1",
    "ru", "sg2", "tr1", "tw2", "vn2",
]

DIVISIONES = [
    ("RANKED_SOLO_5x5", "DIAMOND", "I"),
    ("RANKED_SOLO_5x5", "DIAMOND", "II"),
    ("RANKED_SOLO_5x5", "DIAMOND", "III"),
    ("RANKED_SOLO_5x5", "DIAMOND", "IV"),
    ("RANKED_SOLO_5x5", "PLATINUM", "I"),
    ("RANKED_SOLO_5x5", "PLATINUM", "II"),
    ("RANKED_SOLO_5x5", "PLATINUM", "III"),
    ("RANKED_SOLO_5x5", "PLATINUM", "IV"),
]


def muestrear_ligas(conn):
    total = 0
    for platform in SERVIDORES:
        for queue, tier, division in DIVISIONES:
            puuids = get_puuids_by_tier(queue, tier, division, platform=platform)
            for puuid in puuids:
                guardar_puuid(conn, puuid, platform, tier)
            total += len(puuids)
            time.sleep(ESPERA)
    return total

def actualizar_rangos_pendientes(conn, limite=20):
    cursor = conn.execute(
        "SELECT puuid, region FROM puuids_conocidos WHERE liga IS NULL LIMIT ?",
        (limite,)
    )
    pendientes = cursor.fetchall()

    for puuid, region in pendientes:
        liga = get_rank_by_puuid(puuid)
        conn.execute(
            "UPDATE puuids_conocidos SET liga = ?, fecha_consultado = datetime('now') WHERE puuid = ?",
            (liga, puuid)
        )
        conn.commit()
        time.sleep(ESPERA)

    return len(pendientes)


def run(count_por_jugador=5, limite_puuids_challenger=None, limite_revision=20):
    conn = get_conn()

    # 1. Refrescar la semilla de Challenger
    puuids_challenger = get_puuids_challenger()
    if limite_puuids_challenger:
        puuids_challenger = puuids_challenger[:limite_puuids_challenger]
    logger.info(f"Puuids de Challenger obtenidos: {len(puuids_challenger)}")

    for puuid in puuids_challenger:
        liga = get_rank_by_puuid(puuid)
        guardar_puuid(conn, puuid, "americas", liga)
        time.sleep(ESPERA)
    logger.info(f"Rangos consultados y guardados para {len(puuids_challenger)} puuids de Challenger")

    # 2. Traer la cola de revision (rotacion: nunca revisados primero, luego mas antiguos)
    puuids_a_revisar = obtener_puuids_para_revisar(conn, limite=limite_revision)
    logger.info(f"Puuids seleccionados para revision esta corrida: {len(puuids_a_revisar)}")

    todos_los_ids = set()
    for puuid in puuids_a_revisar:
        ids = get_match_ids(puuid, count=count_por_jugador)
        todos_los_ids.update(ids)
        marcar_puuid_revisado(conn, puuid)
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
                descubrir_puuids(conn, detalle, "americas")
                subidos += 1
                logger.info(f"[{i}/{len(nuevos)}] Subida OK: {match_id}")
            else:
                logger.warning(f"[{i}/{len(nuevos)}] Sin detalle (404): {match_id}")
        except Exception as e:
            logger.error(f"[{i}/{len(nuevos)}] Fallo procesando {match_id}: {e}")
        time.sleep(ESPERA)

    logger.info(f"Partidas subidas a raw: {subidos}")

    actualizados = actualizar_rangos_pendientes(conn, limite=20)
    logger.info(f"Rangos actualizados (snowball): {actualizados}")

    conn.close()


if __name__ == "__main__":
    run(count_por_jugador=3, limite_puuids_challenger=5, limite_revision=10)