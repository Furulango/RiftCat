"""Tareas de mantenimiento del estado.

Uso:
    python -m core.mantenimiento rellenar-keys   # llena s3_key de partidas viejas (una sola vez)
    python -m core.mantenimiento reconciliar     # compara SQLite contra lo que hay en raw

SQLite es la fuente de verdad del pipeline. Reconciliar de vez en cuando
confirma que sigue coincidiendo con el bucket.
"""
import argparse
import time

import boto3

from core.db import get_conn, guardar_s3_keys, keys_registradas
from core.logger_config import get_logger

logger = get_logger(__name__)

RAW_BUCKET = "riftcat-raw"
RAW_PREFIJO = "match_v5/"


def listar_raw(s3):
    """{match_id: key} de todo raw, usando ListObjects (paginas de 1000, sin abrir archivos)."""
    keys = {}
    paginador = s3.get_paginator("list_objects_v2")
    for pagina in paginador.paginate(Bucket=RAW_BUCKET, Prefix=RAW_PREFIJO):
        for objeto in pagina.get("Contents", []):
            key = objeto["Key"]
            if key.endswith(".parquet"):
                keys[key.rsplit("/", 1)[-1].removesuffix(".parquet")] = key
    return keys


def comparar(en_raw, en_sqlite):
    """en_raw: {match_id: key}. en_sqlite: {match_id: s3_key o None}."""
    return {
        "en_raw_sin_registro": sorted(set(en_raw) - set(en_sqlite)),
        "registradas_sin_archivo": sorted(set(en_sqlite) - set(en_raw)),
        "sin_key": sorted(m for m, k in en_sqlite.items() if k is None and m in en_raw),
        "key_distinta": sorted(
            m for m, k in en_sqlite.items() if k is not None and m in en_raw and en_raw[m] != k
        ),
    }


def rellenar_keys(conn, s3):
    inicio = time.time()
    en_raw = listar_raw(s3)
    llenadas = guardar_s3_keys(conn, en_raw)
    print(f"Raw listado en {time.time() - inicio:.1f}s ({len(en_raw)} archivos). s3_key llenadas: {llenadas}")


def reconciliar(conn, s3):
    inicio = time.time()
    resultado = comparar(listar_raw(s3), keys_registradas(conn))
    print(f"Reconciliacion en {time.time() - inicio:.1f}s")
    todo_bien = True
    for nombre, ids in resultado.items():
        print(f"  {nombre}: {len(ids)}" + (f"   ej: {ids[:5]}" if ids else ""))
        todo_bien = todo_bien and not ids
    print("OK: SQLite y raw coinciden" if todo_bien else "Hay diferencias, revisa arriba")
    return resultado


def main():
    parser = argparse.ArgumentParser(description="Mantenimiento del estado de RiftCat")
    parser.add_argument("tarea", choices=["rellenar-keys", "reconciliar"])
    args = parser.parse_args()

    conn = get_conn()
    s3 = boto3.client("s3")
    if args.tarea == "rellenar-keys":
        rellenar_keys(conn, s3)
    else:
        reconciliar(conn, s3)
    conn.close()


if __name__ == "__main__":
    main()
