#!/bin/bash
# reset_completo.sh
# Borra riftcat.db, los logs y TODO el contenido de los buckets raw y curated.
# Uso: ./bash_script/reset_completo.sh

set -e

read -r -p "Esto borra la base, los logs y todos los datos de raw y curated. Escribe BORRAR para continuar: " confirmacion
if [ "$confirmacion" != "BORRAR" ]; then
    echo "Cancelado."
    exit 1
fi

echo "Borrando riftcat.db y logs..."
rm -f riftcat.db riftcat.log transform.log

echo "Borrando contenido de los buckets S3/MinIO..."
python -c "
import boto3
s3 = boto3.client('s3')
paginador = s3.get_paginator('list_objects_v2')
for bucket in ['riftcat-raw', 'riftcat-curated']:
    total = 0
    for pagina in paginador.paginate(Bucket=bucket):
        objetos = [{'Key': o['Key']} for o in pagina.get('Contents', [])]
        if objetos:
            s3.delete_objects(Bucket=bucket, Delete={'Objects': objetos, 'Quiet': True})
            total += len(objetos)
    print(f'{bucket}: {total} objetos borrados')
"

echo "Reset completo."
