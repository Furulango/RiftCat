import pyarrow as pa
import pyarrow.parquet as pq

tabla = pa.table({
    "match_id": ["LA1_1", "LA1_1", "LA1_1", "LA1_1"],
    "team_id": [100, 100, 200, 200],
    "campeon": ["Ahri", "Garen", "Zed", "Lux"],
    "gano": [True, True, False, False],
})

pq.write_table(tabla, "/tmp/prueba.parquet")
print(pq.read_table("/tmp/prueba.parquet").to_pandas())