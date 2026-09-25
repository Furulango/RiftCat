# bash_script/muestrear_ligas.sh
python -c "
from core.db import get_conn
from core.poller import muestrear_ligas

conn = get_conn()
total = muestrear_ligas(conn)
print(f'Puuids descubiertos: {total}')
conn.close()
"
