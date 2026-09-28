import json
import logging
import os
import time

import boto3
import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from core.db import get_conn, filtrar_no_transformados, marcar_transformado

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


def listar_match_ids_en_raw(con):
    logger.info("Listando archivos en raw (solo nombres, sin leer contenido)...")
    inicio = time.time()
    filas = con.execute("""
        SELECT filename
        FROM read_parquet('s3://riftcat-raw/match_v5/*/*/*/*.parquet', filename=true)
    """).fetchall()
    logger.info(f"Listado terminado en {time.time() - inicio:.1f}s. Archivos encontrados: {len(filas)}")

    match_ids = []
    for (ruta,) in filas:
        nombre_archivo = ruta.rsplit("/", 1)[-1]
        match_id = nombre_archivo.removesuffix(".parquet")
        match_ids.append(match_id)
    return match_ids


def leer_payloads_de_raw(con, match_ids_pendientes):
    if not match_ids_pendientes:
        return []

    con.execute("CREATE OR REPLACE TEMP TABLE pendientes (match_id VARCHAR)")
    con.executemany(
        "INSERT INTO pendientes VALUES (?)",
        [(m,) for m in match_ids_pendientes]
    )

    resultado = con.execute("""
        SELECT payload
        FROM read_parquet('s3://riftcat-raw/match_v5/*/*/*/*.parquet', filename=true) AS raw
        JOIN pendientes
            ON regexp_extract(raw.filename, '([^/]+)\\.parquet$', 1) = pendientes.match_id
    """).fetchall()

    return [fila[0] for fila in resultado]


def descargar_curated_existente(s3):
    try:
        s3.download_file(CURATED_BUCKET, CURATED_KEY, TEMP_CURATED_ACTUAL)
    except s3.exceptions.ClientError:
        return None
    return pq.read_table(TEMP_CURATED_ACTUAL)


def _dividir_en_lotes(lista, tamano):
    for i in range(0, len(lista), tamano):
        yield lista[i:i + tamano]


def procesar_lote(con, s3, conn, rangos, lote_match_ids, numero_lote, total_lotes):
    logger.info(f"--- Lote {numero_lote}/{total_lotes}: {len(lote_match_ids)} partidas ---")
    print(f"--- Lote {numero_lote}/{total_lotes}: {len(lote_match_ids)} partidas ---")

    inicio = time.time()
    payloads = leer_payloads_de_raw(con, lote_match_ids)
    logger.info(f"Lote {numero_lote}: payloads leidos en {time.time() - inicio:.1f}s")

    filas_nuevas = []
    for payload in payloads:
        detalle = json.loads(payload)
        filas = extraer_filas(detalle)
        for fila in filas:
            fila["liga"] = rangos.get(fila["puuid"])
        filas_nuevas.extend(filas)

    if not filas_nuevas:
        logger.warning(f"Lote {numero_lote}: no se extrajo ninguna fila, se omite subida")
        return

    columnas = {clave: [fila[clave] for fila in filas_nuevas] for clave in filas_nuevas[0].keys()}
    tabla_nueva = pa.table(columnas)

    tabla_existente = descargar_curated_existente(s3)
    if tabla_existente is not None:
        tabla_final = pa.concat_tables([tabla_existente, tabla_nueva])
    else:
        tabla_final = tabla_nueva

    pq.write_table(tabla_final, TEMP_CURATED_NUEVO)
    s3.upload_file(TEMP_CURATED_NUEVO, CURATED_BUCKET, CURATED_KEY)

    for match_id in lote_match_ids:
        marcar_transformado(conn, match_id)

    logger.info(f"Lote {numero_lote}/{total_lotes} OK. Curated ahora tiene {tabla_final.num_rows} filas totales.")
    print(f"Lote {numero_lote}/{total_lotes} OK. Curated ahora tiene {tabla_final.num_rows} filas totales.")


def run(batch_size=TAMANO_LOTE):
    logger.info(f"=== Iniciando transform.py (incremental, batch_size={batch_size}) ===")
    conn = get_conn()
    rangos = cargar_rangos(conn)
    logger.info(f"Rangos cargados de la DB: {len(rangos)} puuids con liga conocida")

    con = _conectar_duckdb()
    s3 = boto3.client("s3")

    todos_los_match_ids = listar_match_ids_en_raw(con)
    print(f"Match ids totales en raw: {len(todos_los_match_ids)}")

    pendientes = filtrar_no_transformados(conn, todos_los_match_ids)
    logger.info(f"Match ids pendientes de transformar: {len(pendientes)}")
    print(f"Match ids pendientes de transformar: {len(pendientes)}")

    if not pendientes:
        logger.info("Nada nuevo que transformar. Saliendo.")
        print("Nada nuevo que transformar. Saliendo.")
        conn.close()
        return

    lotes = list(_dividir_en_lotes(pendientes, batch_size))
    total_lotes = len(lotes)
    logger.info(f"Se procesaran {len(pendientes)} partidas en {total_lotes} lotes de hasta {batch_size}")
    print(f"Se procesaran {len(pendientes)} partidas en {total_lotes} lotes de hasta {batch_size}")

    for i, lote in enumerate(lotes, start=1):
        procesar_lote(con, s3, conn, rangos, lote, i, total_lotes)

    conn.close()
    logger.info(f"=== Todos los lotes completados. {len(pendientes)} partidas incorporadas en total. ===")
    print(f"Todos los lotes completados. {len(pendientes)} partidas incorporadas en total.")


if __name__ == "__main__":
    run()