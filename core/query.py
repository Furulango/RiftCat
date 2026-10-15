"""Consulta rapida de ejemplo sobre curated: campeones mas jugados y su winrate.
Uso: python -m core.query"""
import os

import duckdb

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

filas = con.execute("""
    SELECT champion_name,
           count(*) AS partidas,
           round(100.0 * avg(win::INT), 1) AS winrate
    FROM 's3://riftcat-curated/match_v5/curated.parquet'
    GROUP BY champion_name
    ORDER BY partidas DESC
    LIMIT 20
""").fetchall()

for campeon, partidas, winrate in filas:
    print(f"{campeon:<15}{partidas:>8}{winrate:>7}%")
