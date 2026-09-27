import json
import boto3
import duckdb
import os
import pyarrow as pa
import pyarrow.parquet as pq

from core.db import get_conn


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


def leer_payloads_de_raw():
    con = duckdb.connect()
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

    resultado = con.execute(
        "SELECT payload FROM 's3://riftcat-raw/match_v5/*/*/*/*.parquet'"
    ).fetchall()
    return [fila[0] for fila in resultado]


def run():
    conn = get_conn()
    rangos = cargar_rangos(conn)

    payloads = leer_payloads_de_raw()
    print(f"Partidas leidas de raw: {len(payloads)}")

    todas_las_filas = []
    for payload in payloads:
        detalle = json.loads(payload)
        filas = extraer_filas(detalle)
        for fila in filas:
            fila["liga"] = rangos.get(fila["puuid"])
        todas_las_filas.extend(filas)

    print(f"Filas totales (jugador-partida): {len(todas_las_filas)}")

    columnas = {clave: [fila[clave] for fila in todas_las_filas] for clave in todas_las_filas[0].keys()}
    tabla = pa.table(columnas)
    pq.write_table(tabla, "/tmp/curated_completo.parquet")

    s3 = boto3.client("s3")
    s3.upload_file("/tmp/curated_completo.parquet", "riftcat-curated", "match_v5/curated.parquet")

    conn.close()
    print("Subido a curated OK")


if __name__ == "__main__":
    run()
