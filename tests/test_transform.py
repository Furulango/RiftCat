import pytest

from core import db, transform


@pytest.fixture
def conn(tmp_path, monkeypatch):
    monkeypatch.setattr(db, "DB_PATH", str(tmp_path / "test.db"))
    c = db.get_conn()
    yield c
    c.close()


def test_rutas_pendientes_salen_de_sqlite(conn):
    db.marcar_visto(conn, "LA1_1", "match_v5/platform=LA1/dt=2026-10-15/h=ab/LA1_1.parquet")
    db.marcar_visto(conn, "LA1_2")
    db.marcar_visto(conn, "LA1_3", "k3")
    db.marcar_transformado(conn, "LA1_3")

    rutas, sin_key = transform.obtener_rutas_pendientes(conn)
    assert rutas == {"LA1_1": "s3://riftcat-raw/match_v5/platform=LA1/dt=2026-10-15/h=ab/LA1_1.parquet"}
    assert sin_key == ["LA1_2"]


def test_extraer_filas_sin_participantes_devuelve_vacio():
    assert transform.extraer_filas({"metadata": {"matchId": "X"}, "info": {"participants": []}}) == []


def _payload(match_id, n_jugadores):
    import json
    participantes = [
        {"puuid": f"{match_id}-p{i}", "teamId": 100, "championName": "Ahri", "teamPosition": "MIDDLE",
         "win": True, "kills": 1, "deaths": 1, "assists": 1, "goldEarned": 1,
         "totalDamageDealtToChampions": 1, "visionScore": 1}
        for i in range(n_jugadores)
    ]
    return json.dumps({"metadata": {"matchId": match_id}, "info": {"participants": participantes}})


class S3Falso:
    def __init__(self):
        self.subidas = 0

    class exceptions:
        ClientError = FileNotFoundError

    def download_file(self, bucket, key, destino):
        raise FileNotFoundError

    def upload_file(self, origen, bucket, key):
        self.subidas += 1


def test_procesar_lote_marca_vacios_y_cuenta_solo_los_reales(conn, monkeypatch, tmp_path):
    monkeypatch.setattr(transform, "TEMP_CURATED_NUEVO", str(tmp_path / "curated.parquet"))
    monkeypatch.setattr(transform, "leer_payloads_de_raw", lambda con, rutas: [
        ("LA1_1", _payload("LA1_1", 10)),
        ("LA1_2", _payload("LA1_2", 0)),
    ])
    s3 = S3Falso()
    lote = ["LA1_1", "LA1_2", "LA1_3"]  # LA1_3 no se pudo leer

    incorporadas = transform.procesar_lote(None, s3, conn, {}, lote, ["r1", "r2", "r3"], 1, 1)

    assert incorporadas == 1
    assert s3.subidas == 1
    marcadas = {r[0] for r in conn.execute("SELECT match_id FROM matches_en_curated")}
    assert marcadas == {"LA1_1", "LA1_2"}


def test_lote_solo_con_vacios_no_sube_pero_los_marca(conn, monkeypatch):
    monkeypatch.setattr(transform, "leer_payloads_de_raw", lambda con, rutas: [("LA1_2", _payload("LA1_2", 0))])
    s3 = S3Falso()
    assert transform.procesar_lote(None, s3, conn, {}, ["LA1_2"], ["r2"], 1, 1) == 0
    assert s3.subidas == 0
    assert {r[0] for r in conn.execute("SELECT match_id FROM matches_en_curated")} == {"LA1_2"}
