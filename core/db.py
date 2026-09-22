import sqlite3

DB_PATH = "riftcat.db"


def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS matches_vistos (
            match_id TEXT PRIMARY KEY,
            fecha_procesado TEXT
        )
    """)
    return conn


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


def marcar_visto(conn, match_id):
    conn.execute(
        "INSERT OR IGNORE INTO matches_vistos (match_id, fecha_procesado) VALUES (?, datetime('now'))",
        (match_id,)
    )
    conn.commit()
