
import boto3
import pyarrow as pa
import pyarrow.parquet as pq
import json

import hashlib
from datetime import datetime


def calcular_hash_prefix(match_id):
    return hashlib.md5(match_id.encode()).hexdigest()[:2]

def armar_registro(match_id, detalle):
    return {
        "match_id": match_id,
        "hash_prefix": calcular_hash_prefix(match_id),
        "ingested_at": datetime.now().isoformat(),
        "payload": json.dumps(detalle),
    }

def guardar_parquet(registro, path):

    tabla = pa.table({
        "match_id": [registro["match_id"]],
        "hash_prefix": [registro["hash_prefix"]],
        "ingested_at": [registro["ingested_at"]],
        "payload": [registro["payload"]],
    })

    pq.write_table(tabla, path)

def subir_a_raw(path_local, match_id, fecha):
    platform = match_id.split("_")[0]
    hash_prefix = calcular_hash_prefix(match_id)
    key_destino = f"match_v5/platform={platform}/dt={fecha}/h={hash_prefix}/{match_id}.parquet"

    s3 = boto3.client("s3")
    s3.upload_file(path_local, "riftcat-raw", key_destino)
    return key_destino
