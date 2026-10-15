import json
import logging
import os
import time

import boto3
import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from core.db import get_conn, marcar_transformado, pendientes_de_transformar

logger = logging.getLogger("transform")
logger.setLevel(logging.INFO)
if not logger.handlers:
    _handler = logging.FileHandler("transform.log")
    _handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s [%(name)s] %(message)s"))
    logger.addHandler(_handler)

CURATED_BUCKET = "riftcat-curated"
CURATED_KEY = "match_v5/curated.parquet"
TEMP_CURATED_ACTUAL = "/tmp/curated_actual.parquet"
TEMP_CURATED_NUEVO = "/tmp/curated_completo.parquet"

TAMANO_LOTE = 1000

SCHEMA_CURATED = pa.schema([
    ("match_id", pa.string()),
    ("puuid", pa.string()),
    ("team_id", pa.int64()),
    ("champion_name", pa.string()),
    ("team_position", pa.string()),
    ("win", pa.bool_()),
    ("kills", pa.int64()),
    ("deaths", pa.int64()),
    ("assists", pa.int64()),
    ("gold_earned", pa.int64()),
    ("total_damage_to_champions", pa.int64()),
    ("vision_score", pa.int64()),
    ("liga", pa.string()),
])


def extraer_filas(detalle):
    filas = []
    match_id = detalle.get("metadata", {}).get("matchId")

    for participante in detalle["info"]["participants"]:
        fila = {
            "match_id": match_id,
            "puuid": participante.get("puuid"),
            "team_id": participante.get("teamId"),
            "champion_name": participante.get("championName"),
            "team_position": participante.get("teamPosition"),
            "win": participante.get("win"),
            "kills": participante.get("kills"),
            "deaths": participante.get("deaths"),
            "assists": participante.get("assists"),
            "gold_earned": participante.get("goldEarned"),
            "total_damage_to_champions": participante.get("totalDamageDealtToChampions"),
            "vision_score": participante.get("visionScore"),
        }
        filas.append(fila)
    return filas


def cargar_rangos(conn):
    cursor = conn.execute("SELECT puuid, liga FROM puuids_conocidos")
    return {puuid: liga for puuid, liga in cursor.fetchall()}


def _conectar_duckdb():
    logger.info("Conectando a duckdb y configurando acceso a S3/MinIO")
    con = duckdb.connect()
    con.execute("SET threads=1")
    con.execute("INSTALL httpfs")
    con.execute("LOAD httpfs")

    endpoint = os.environ["AWS_ENDPOINT_URL_S3"].replace("http://", "")
    con.execute(f"""
        CREATE SECRET (
            TYPE S3,
            KEY_ID '{os.environ["AWS_ACCESS_KEY_ID"]}',
            SECRET '{os.environ["AWS_SECRET_ACCESS_KEY"]}',
            ENDPOINT '{endpoint}',
            URL_STYLE 'path',
            USE_SSL false
        )
    """)
    return con


RAW_BUCKET = "riftcat-raw"


def obtener_rutas_pendientes(conn):
    """Lo que falta transformar, segun SQLite. No lista raw: la ruta exacta se guardo al subir.
    Devuelve ({match_id: ruta s3}, [match_ids sin s3_key])."""
    rutas = {}
    sin_key = []
    for match_id, s3_key in pendientes_de_transformar(conn):
        if s3_key:
            rutas[match_id] = f"s3://{RAW_BUCKET}/{s3_key}"
        else:
            sin_key.append(match_id)
    return rutas, sin_key


def leer_payloads_de_raw(con, rutas):
    if not rutas:
        return []
    lista = ", ".join("'" + r.replace("'", "''") + "'" for r in rutas)
    resultado = con.execute(f"SELECT filename, payload FROM read_parquet([{lista}], filename=true)").fetchall()
    return [(ruta.rsplit("/", 1)[-1].removesuffix(".parquet"), payload) for ruta, payload in resultado]



def descargar_curated_existente(s3):
    try:
        s3.download_file(CURATED_BUCKET, CURATED_KEY, TEMP_CURATED_ACTUAL)
    except s3.exceptions.ClientError:
        return None
    return pq.read_table(TEMP_CURATED_ACTUAL)


def _dividir_en_lotes(lista, tamano):
    for i in range(0, len(lista), tamano):
        yield lista[i:i + tamano]


def procesar_lote(con, s3, conn, rangos, lote_match_ids, rutas_lote, numero_lote, total_lotes):
    logger.info(f"--- Lote {numero_lote}/{total_lotes}: {len(lote_match_ids)} partidas ---")

    inicio = time.time()
    payloads = leer_payloads_de_raw(con, rutas_lote)
    logger.info(f"Lote {numero_lote}: {len(payloads)} payloads leidos en {time.time() - inicio:.1f}s")

    filas_nuevas = []
    procesados = set()
    vacios = set()
    for match_id, payload in payloads:
        detalle = json.loads(payload)
        filas = extraer_filas(detalle)
        if not filas:
            vacios.add(match_id)
            continue
        for fila in filas:
            fila["liga"] = rangos.get(fila["puuid"])
        filas_nuevas.extend(filas)
        procesados.add(match_id)

    # Partidas que Riot devolvio sin participantes: no aportan filas, pero se marcan
    # para que no se reintenten en cada corrida.
    for match_id in vacios:
        marcar_transformado(conn, match_id)
    if vacios:
        logger.warning(f"Lote {numero_lote}: {len(vacios)} partidas sin participantes, marcadas y omitidas: {sorted(vacios)}")

    faltantes = len(lote_match_ids) - len(procesados) - len(vacios)
    if faltantes:
        logger.warning(f"Lote {numero_lote}: {faltantes} partidas no se pudieron leer, se reintentaran en la siguiente corrida")

    if not filas_nuevas:
        logger.warning(f"Lote {numero_lote}: no se extrajo ninguna fila, se omite subida")
        return 0

    tabla_nueva = pa.Table.from_pylist(filas_nuevas, schema=SCHEMA_CURATED)

    tabla_existente = descargar_curated_existente(s3)
    if tabla_existente is not None:
        tabla_final = pa.concat_tables([tabla_existente.cast(SCHEMA_CURATED), tabla_nueva])
    else:
        tabla_final = tabla_nueva
        
    pq.write_table(tabla_final, TEMP_CURATED_NUEVO)
    s3.upload_file(TEMP_CURATED_NUEVO, CURATED_BUCKET, CURATED_KEY)

    for match_id in procesados:
        marcar_transformado(conn, match_id)

    logger.info(f"Lote {numero_lote}/{total_lotes} OK. Curated ahora tiene {tabla_final.num_rows} filas totales.")
    return len(procesados)


def run(batch_size=TAMANO_LOTE):
    logger.info(f"=== Iniciando transform.py (incremental, batch_size={batch_size}) ===")
    conn = get_conn()
    rangos = cargar_rangos(conn)
    logger.info(f"Rangos cargados de la DB: {sum(1 for v in rangos.values() if v)} puuids con liga conocida")

    con = _conectar_duckdb()
    s3 = boto3.client("s3")

    rutas, sin_key = obtener_rutas_pendientes(conn)
    if sin_key:
        logger.warning(
            f"{len(sin_key)} partidas en raw sin s3_key registrada; se omiten. "
            "Corre: python -m core.mantenimiento rellenar-keys"
        )
    pendientes = list(rutas.keys())
    logger.info(f"Match ids pendientes de transformar: {len(pendientes)}")

    if not pendientes:
        logger.info("Nada nuevo que transformar. Saliendo.")
        conn.close()
        return

    lotes = list(_dividir_en_lotes(pendientes, batch_size))
    total_lotes = len(lotes)
    logger.info(f"Se procesaran {len(pendientes)} partidas en {total_lotes} lotes de hasta {batch_size}")

    incorporadas = 0
    for i, lote in enumerate(lotes, start=1):
        rutas_lote = [rutas[m] for m in lote]
        incorporadas += procesar_lote(con, s3, conn, rangos, lote, rutas_lote, i, total_lotes)

    conn.close()
    logger.info(f"=== Todos los lotes completados. {incorporadas} partidas incorporadas a curated. ===")
    print(f"Todos los lotes completados. {incorporadas} partidas incorporadas a curated.")


if __name__ == "__main__":
    run()