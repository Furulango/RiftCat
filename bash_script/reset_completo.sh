#!/bin/bash
# reset_completo.sh
# Borra riftcat.db, riftcat.log, y el contenido de los buckets raw/curated.
# Uso: ./bash_script/reset_completo.sh

set -e

echo "Borrando riftcat.db y riftcat.log..."
rm -f riftcat.db riftcat.log

echo "Borrando contenido de los buckets S3/MinIO..."
python -c "
import boto3
s3 = boto3.client('s3')
for bucket in ['riftcat-raw', 'riftcat-curated']:
    objetos = s3.list_objects_v2(Bucket=bucket).get('Contents', [])
    for o in objetos:
        s3.delete_object(Bucket=bucket, Key=o['Key'])
    print(f'{bucket}: {len(objetos)} objetos borrados')
"

echo "Reset completo."
