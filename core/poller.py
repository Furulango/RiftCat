"""Ingesta de partidas desde la API de Riot hacia raw.

La ingesta va en dos pasos desacoplados por la cola match_queue (SQLite):

    descubrir  ->  match_queue  ->  worker por region  ->  raw

Comandos:
    python -m core.poller muestrear                  # siembra puuids desde League-V4
    python -m core.poller descubrir --limite 100     # busca match_ids de puuids y los encola
    python -m core.poller worker --region sea        # descarga lo pendiente de una region
    python -m core.poller rangos --limite 200        # consulta el rango de puuids sin liga
    python -m core.poller ciclo                      # descubrir + un worker por region en paralelo

Por que un worker por region: el rate limit de Match-V5 se cuenta por region de
ruteo (americas, europe, asia, sea), asi que 4 workers en paralelo no compiten
entre si. Mas workers en la misma region solo se reparten la misma cuota.
"""
import argparse
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

from core.db import (
    get_conn, guardar_puuid, descubrir_puuids, obtener_puuids_para_revisar,
    marcar_puuid_revisado, marcar_visto, encolar, obtener_pendientes,
    marcar_subido, marcar_fallo, resumen_cola,
)
from core.logger_config import get_logger
from core.parquet_writer import armar_registro, guardar_parquet, subir_a_raw
from core.riot_client import (
    get_puuids_challenger, get_match_ids, get_match_detail,
    get_rank_by_puuid, get_puuids_by_tier, region_de_plataforma,
    plataformas_de_region, KeyInvalidaError, RiotError,
)

logger = get_logger(__name__)

ESPERA = 1.2  # ~0.83 req/s: queda bajo 100 req / 2 min por routing value
MAX_FALLOS_SEGUIDOS = 10
REGIONES = ["americas", "europe", "asia", "sea"]
SIN_RANGO = "UNRANKED"

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
    ("RANKED_SOLO_5x5", "EMERALD", "I"),
    ("RANKED_SOLO_5x5", "EMERALD", "II"),
    ("RANKED_SOLO_5x5", "EMERALD", "III"),
    ("RANKED_SOLO_5x5", "EMERALD", "IV"),
    ("RANKED_SOLO_5x5", "PLATINUM", "I"),
    ("RANKED_SOLO_5x5", "PLATINUM", "II"),
    ("RANKED_SOLO_5x5", "PLATINUM", "III"),
    ("RANKED_SOLO_5x5", "PLATINUM", "IV"),
]


class RiotCaidoError(Exception):
    """Demasiados fallos seguidos en una region: Riot probablemente esta caido ahi."""


# ---------- siembra ----------

def muestrear_ligas(conn):
    total = 0

    puuids_challenger = get_puuids_challenger()
    for puuid in puuids_challenger:
        guardar_puuid(conn, puuid, "la1", "CHALLENGER")
    total += len(puuids_challenger)
    logger.info(f"Challenger: {len(puuids_challenger)} puuids")

    for platform in SERVIDORES:
        for queue, tier, division in DIVISIONES:
            puuids = get_puuids_by_tier(queue, tier, division, platform=platform)
            for puuid in puuids:
                guardar_puuid(conn, puuid, platform, tier)
            total += len(puuids)
            logger.info(f"{platform} {tier} {division}: {len(puuids)} puuids")
            time.sleep(ESPERA)

    logger.info(f"Muestreo de ligas completo: {total} puuids procesados")
    return total


# ---------- paso 1: descubrir ----------

def descubrir(conn, limite=100, count_por_jugador=5):
    """Busca las partidas recientes de los puuids revisados hace mas tiempo y las encola.
    El puuid se marca como revisado solo despues de encolar sus partidas."""
    puuids = obtener_puuids_para_revisar(conn, limite=limite)
    logger.info(f"descubrir: {len(puuids)} puuids a revisar")

    encolados = 0
    for puuid, platform in puuids:
        try:
            region = region_de_plataforma(platform)
            ids = get_match_ids(puuid, region=region, count=count_por_jugador)
            encolados += encolar(conn, ids, puuid_origen=puuid)
            marcar_puuid_revisado(conn, puuid)
        except KeyInvalidaError:
            raise
        except ValueError as e:
            logger.error(f"descubrir: plataforma invalida para {puuid}: {e}")
            marcar_puuid_revisado(conn, puuid)
        except RiotError as e:
            logger.error(f"descubrir: no se pudo consultar {puuid} ({platform}), queda para la siguiente: {e}")
        time.sleep(ESPERA)

    logger.info(f"descubrir: {encolados} match_ids nuevos en cola. Cola: {resumen_cola(conn)}")
    return encolados


# ---------- paso 2: worker por region ----------

def worker(conn, region, limite=None):
    """Descarga las partidas pendientes de una region de ruteo y las sube a raw."""
    plataformas = plataformas_de_region(region)
    cola = obtener_pendientes(conn, plataformas, limite=limite)
    logger.info(f"worker {region}: {len(cola)} partidas pendientes")

    temp_path = f"/tmp/riftcat_{region}.parquet"
    fecha = datetime.now().strftime("%Y-%m-%d")
    subidas = 0
    fallos_seguidos = 0

    for i, (match_id, plataforma) in enumerate(cola, start=1):
        try:
            detalle = get_match_detail(match_id, region=region)
            if detalle is None:
                marcar_fallo(conn, match_id, "404: no existe", definitivo=True)
            else:
                registro = armar_registro(match_id, detalle)
                guardar_parquet(registro, temp_path)
                s3_key = subir_a_raw(temp_path, match_id, fecha)
                marcar_visto(conn, match_id, s3_key)
                descubrir_puuids(conn, detalle, plataforma)
                marcar_subido(conn, match_id)
                subidas += 1
            fallos_seguidos = 0
        except KeyInvalidaError:
            logger.error(f"worker {region}: key invalida o expirada. Deteniendo.")
            raise
        except Exception as e:
            fallos_seguidos += 1
            marcar_fallo(conn, match_id, e)
            logger.error(f"worker {region}: [{i}/{len(cola)}] fallo {match_id}: {e}")
            if fallos_seguidos >= MAX_FALLOS_SEGUIDOS:
                raise RiotCaidoError(f"{region}: {fallos_seguidos} fallos seguidos, ultimo: {e}")

        if i % 50 == 0:
            logger.info(f"worker {region}: [{i}/{len(cola)}] subidas: {subidas}")
        time.sleep(ESPERA)

    logger.info(f"worker {region}: terminado. Subidas: {subidas} de {len(cola)}")
    return subidas


def _worker_con_conexion(region, limite):
    """Cada hilo abre su propia conexion: sqlite3 no comparte conexiones entre hilos."""
    conn = get_conn()
    try:
        return worker(conn, region, limite)
    except RiotCaidoError as e:
        logger.error(f"worker {region}: se detuvo, lo pendiente queda en cola: {e}")
        return 0
    finally:
        conn.close()


def workers_en_paralelo(regiones, limite=None):
    with ThreadPoolExecutor(max_workers=len(regiones)) as pool:
        resultados = dict(zip(regiones, pool.map(lambda r: _worker_con_conexion(r, limite), regiones)))
    logger.info(f"workers: subidas por region {resultados}")
    return resultados


# ---------- rangos ----------

def actualizar_rangos_pendientes(conn, limite=20):
    """Consulta el rango de puuids sin liga, empezando por los consultados hace mas tiempo.
    Si el jugador no tiene rango en solo/duo se guarda UNRANKED, para que no vuelva
    a salir en cada corrida."""
    cursor = conn.execute(
        """
        SELECT puuid, region FROM puuids_conocidos
        WHERE liga IS NULL
        ORDER BY fecha_consultado ASC
        LIMIT ?
        """,
        (limite,)
    )
    pendientes = cursor.fetchall()

    actualizados = 0
    for puuid, platform in pendientes:
        try:
            liga = get_rank_by_puuid(puuid, platform=platform) or SIN_RANGO
            conn.execute(
                "UPDATE puuids_conocidos SET liga = ?, fecha_consultado = datetime('now') WHERE puuid = ?",
                (liga, puuid)
            )
            actualizados += 1
        except KeyInvalidaError:
            raise
        except (RiotError, ValueError) as e:
            logger.error(f"No se pudo consultar el rango de {puuid} ({platform}): {e}")
            conn.execute(
                "UPDATE puuids_conocidos SET fecha_consultado = datetime('now') WHERE puuid = ?",
                (puuid,)
            )
        conn.commit()
        time.sleep(ESPERA)

    return actualizados


# ---------- CLI ----------

def main(argv=None):
    parser = argparse.ArgumentParser(description="Ingesta de RiftCat")
    sub = parser.add_subparsers(dest="comando", required=True)

    sub.add_parser("muestrear", help="Siembra puuids desde League-V4")

    p = sub.add_parser("descubrir", help="Encola match_ids de los puuids revisados hace mas tiempo")
    p.add_argument("--limite", type=int, default=100, help="puuids a revisar")
    p.add_argument("--count", type=int, default=5, help="partidas recientes por puuid")

    p = sub.add_parser("worker", help="Descarga lo pendiente de una region")
    p.add_argument("--region", required=True, choices=REGIONES)
    p.add_argument("--limite", type=int, default=None, help="maximo de partidas en esta corrida")

    p = sub.add_parser("rangos", help="Consulta el rango de puuids sin liga")
    p.add_argument("--limite", type=int, default=200)

    p = sub.add_parser("ciclo", help="descubrir + un worker por region en paralelo + rangos")
    p.add_argument("--limite-descubrir", type=int, default=100)
    p.add_argument("--limite-worker", type=int, default=500, help="partidas por region")
    p.add_argument("--limite-rangos", type=int, default=50)

    args = parser.parse_args(argv)
    conn = get_conn()
    try:
        if args.comando == "muestrear":
            muestrear_ligas(conn)
        elif args.comando == "descubrir":
            descubrir(conn, limite=args.limite, count_por_jugador=args.count)
        elif args.comando == "worker":
            worker(conn, args.region, limite=args.limite)
        elif args.comando == "rangos":
            logger.info(f"rangos: {actualizar_rangos_pendientes(conn, limite=args.limite)} actualizados")
        elif args.comando == "ciclo":
            descubrir(conn, limite=args.limite_descubrir)
            workers_en_paralelo(REGIONES, limite=args.limite_worker)
            logger.info(f"rangos: {actualizar_rangos_pendientes(conn, limite=args.limite_rangos)} actualizados")
        print(f"Cola: {resumen_cola(conn)}")
        return 0
    except KeyInvalidaError as e:
        logger.error(f"Key invalida o expirada: {e}")
        print(f"Key invalida o expirada: {e}", file=sys.stderr)
        return 1
    except RiotCaidoError as e:
        print(f"Riot parece caido: {e}. Lo pendiente sigue en la cola.", file=sys.stderr)
        return 2
    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(main())
