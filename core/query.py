import os
import duckdb

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

print(con.execute("""
    SELECT campeon, COUNT(*) AS partidas
    FROM 's3://riftcat-curated/match_v5/*/*.parquet'
    GROUP BY campeon
    ORDER BY partidas DESC
""").fetchall())