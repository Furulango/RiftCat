"""Estado operativo de RiftCat en SQLite.

SQLite es la fuente de verdad de que hay en cada capa:
- puuids_conocidos: jugadores descubiertos y cuando se revisaron.
- match_queue: match_ids descubiertos que faltan por descargar.
- matches_vistos: partidas que ya estan en raw, con la key exacta del archivo.
- matches_en_curated: partidas que ya pasaron por el transform.

Este modulo solo habla con SQLite: no sabe de Riot ni de S3.
"""
import sqlite3

DB_PATH = "riftcat.db"

PENDIENTE = "pendiente"
SUBIDO = "subido"
ERROR = "error"
MAX_INTENTOS = 3


def _columnas(conn, tabla):
    return {fila[1] for fila in conn.execute(f"PRAGMA table_info({tabla})").fetchall()}


def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA busy_timeout = 30000")

    conn.execute("""
        CREATE TABLE IF NOT EXISTS matches_vistos (
            match_id TEXT PRIMARY KEY,
            fecha_procesado TEXT,
            s3_key TEXT
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS puuids_conocidos (
            puuid TEXT PRIMARY KEY,
            region TEXT,
            liga TEXT,
            fecha_consultado TEXT,
            fecha_ultimo_check TEXT
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS matches_en_curated (
            match_id TEXT PRIMARY KEY,
            fecha_transformado TEXT
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS match_queue (
            match_id TEXT PRIMARY KEY,
            plataforma TEXT,
            puuid_origen TEXT,
            estado TEXT DEFAULT 'pendiente',
            intentos INTEGER DEFAULT 0,
            fecha_descubierto TEXT,
            ultimo_error TEXT
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_match_queue_estado ON match_queue (estado, plataforma)")

    # Migraciones de bases creadas con versiones anteriores
    if "fecha_ultimo_check" not in _columnas(conn, "puuids_conocidos"):
        conn.execute("ALTER TABLE puuids_conocidos ADD COLUMN fecha_ultimo_check TEXT")
    if "s3_key" not in _columnas(conn, "matches_vistos"):
        conn.execute("ALTER TABLE matches_vistos ADD COLUMN s3_key TEXT")

    conn.commit()
    return conn


# ---------- raw ----------

def filtrar_nuevos(conn, match_ids):
    if not match_ids:
        return []

    placeholders = ",".join("?" * len(match_ids))
    cursor = conn.execute(
        f"SELECT match_id FROM matches_vistos WHERE match_id IN ({placeholders})",
        match_ids
    )
    ya_vistos = {fila[0] for fila in cursor.fetchall()}
    return [m for m in match_ids if m not in ya_vistos]


def marcar_visto(conn, match_id, s3_key=None):
    """Registra que la partida ya esta en raw. Si se pasa s3_key, se guarda la ruta exacta."""
    conn.execute(
        """
        INSERT INTO matches_vistos (match_id, fecha_procesado, s3_key)
        VALUES (?, datetime('now'), ?)
        ON CONFLICT(match_id) DO UPDATE SET s3_key = COALESCE(excluded.s3_key, matches_vistos.s3_key)
        """,
        (match_id, s3_key)
    )
    conn.commit()


def guardar_s3_keys(conn, keys_por_match):
    """Rellena s3_key de partidas que ya estaban registradas sin ella. Devuelve cuantas se llenaron."""
    antes = conn.total_changes
    conn.executemany(
        "UPDATE matches_vistos SET s3_key = ? WHERE match_id = ? AND s3_key IS NULL",
        [(key, match_id) for match_id, key in keys_por_match.items()]
    )
    conn.commit()
    return conn.total_changes - antes


def keys_registradas(conn):
    """Todo lo que SQLite cree que hay en raw: {match_id: s3_key o None}."""
    return dict(conn.execute("SELECT match_id, s3_key FROM matches_vistos").fetchall())


# ---------- puuids ----------

def guardar_puuid(conn, puuid, region, liga):
    conn.execute(
        "INSERT OR IGNORE INTO puuids_conocidos (puuid, region, liga, fecha_consultado) VALUES (?, ?, ?, datetime('now'))",
        (puuid, region, liga)
    )
    conn.commit()


def descubrir_puuids(conn, detalle, region):
    puuids = detalle.get("metadata", {}).get("participants", [])
    for puuid in puuids:
        guardar_puuid(conn, puuid, region, None)


def obtener_puuids_para_revisar(conn, limite=20):
    cursor = conn.execute(
        """
        SELECT puuid, region FROM puuids_conocidos
        ORDER BY fecha_ultimo_check IS NOT NULL, fecha_ultimo_check ASC
        LIMIT ?
        """,
        (limite,)
    )
    return cursor.fetchall()


def marcar_puuid_revisado(conn, puuid):
    conn.execute(
        "UPDATE puuids_conocidos SET fecha_ultimo_check = datetime('now') WHERE puuid = ?",
        (puuid,)
    )
    conn.commit()


# ---------- cola de descarga ----------

def encolar(conn, match_ids, puuid_origen=None):
    """Agrega match_ids a la cola, ignorando los que ya estan en cola o ya en raw.
    Devuelve cuantos se agregaron."""
    antes = conn.total_changes
    for match_id in match_ids:
        plataforma = match_id.split("_")[0].lower()
        conn.execute(
            """
            INSERT OR IGNORE INTO match_queue (match_id, plataforma, puuid_origen, estado, fecha_descubierto)
            SELECT ?, ?, ?, ?, datetime('now')
            WHERE NOT EXISTS (SELECT 1 FROM matches_vistos WHERE match_id = ?)
            """,
            (match_id, plataforma, puuid_origen, PENDIENTE, match_id),
        )
    conn.commit()
    return conn.total_changes - antes


def obtener_pendientes(conn, plataformas, limite=None):
    """Pendientes de las plataformas dadas, los mas viejos primero.
    db.py no sabe de regiones: el que llama decide que plataformas le tocan."""
    if not plataformas:
        return []
    placeholders = ",".join("?" * len(plataformas))
    sql = f"""
        SELECT match_id, plataforma FROM match_queue
        WHERE estado = ? AND plataforma IN ({placeholders})
        ORDER BY fecha_descubierto
    """
    params = [PENDIENTE, *plataformas]
    if limite:
        sql += " LIMIT ?"
        params.append(limite)
    return conn.execute(sql, params).fetchall()


def marcar_subido(conn, match_id):
    conn.execute("UPDATE match_queue SET estado = ? WHERE match_id = ?", (SUBIDO, match_id))
    conn.commit()


def marcar_fallo(conn, match_id, error):
    """Suma un intento. Al llegar a MAX_INTENTOS pasa a 'error' y deja de reintentarse."""
    conn.execute(
        """
        UPDATE match_queue
        SET intentos = intentos + 1,
            ultimo_error = ?,
            estado = CASE WHEN intentos + 1 >= ? THEN ? ELSE ? END
        WHERE match_id = ?
        """,
        (str(error)[:200], MAX_INTENTOS, ERROR, PENDIENTE, match_id),
    )
    conn.commit()


def resumen_cola(conn):
    return dict(conn.execute("SELECT estado, count(*) FROM match_queue GROUP BY 1").fetchall())


# ---------- curated ----------

def pendientes_de_transformar(conn):
    """Partidas en raw que todavia no pasan por el transform: [(match_id, s3_key)].
    s3_key puede ser None en partidas viejas que no se han rellenado."""
    return conn.execute(
        """
        SELECT v.match_id, v.s3_key FROM matches_vistos v
        WHERE NOT EXISTS (SELECT 1 FROM matches_en_curated c WHERE c.match_id = v.match_id)
        ORDER BY v.fecha_procesado
        """
    ).fetchall()


def filtrar_no_transformados(conn, match_ids):
    if not match_ids:
        return []

    placeholders = ",".join("?" * len(match_ids))
    cursor = conn.execute(
        f"SELECT match_id FROM matches_en_curated WHERE match_id IN ({placeholders})",
        match_ids
    )
    ya_transformados = {fila[0] for fila in cursor.fetchall()}
    return [m for m in match_ids if m not in ya_transformados]


def marcar_transformado(conn, match_id):
    conn.execute(
        "INSERT OR IGNORE INTO matches_en_curated (match_id, fecha_transformado) VALUES (?, datetime('now'))",
        (match_id,)
    )
    conn.commit()
