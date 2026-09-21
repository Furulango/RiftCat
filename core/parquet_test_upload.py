import boto3

s3 = boto3.client("s3")
s3.upload_file("/tmp/prueba.parquet", "riftcat-curated", "match_v5/dt=2026-09-21/prueba.parquet")
for o in s3.list_objects_v2(Bucket="riftcat-curated", Prefix="match_v5/")["Contents"]:
    print(o["Key"], o["Size"])