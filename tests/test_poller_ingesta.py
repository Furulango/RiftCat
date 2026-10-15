import pytest

from core import db, poller
from core.riot_client import KeyInvalidaError, RiotNoDisponibleError


@pytest.fixture
def conn(tmp_path, monkeypatch):
    monkeypatch.setattr(db, "DB_PATH", str(tmp_path / "test.db"))
    monkeypatch.setattr(poller.time, "sleep", lambda s: None)
    c = db.get_conn()
    yield c
    c.close()


@pytest.fixture
def sin_s3(monkeypatch):
    """Reemplaza el guardado y la subida a raw; registra que se subio."""
    subidas = []
    monkeypatch.setattr(poller, "guardar_parquet", lambda registro, path: None)

    def subir(path, match_id, fecha):
        subidas.append(match_id)
        return f"match_v5/platform={match_id.split('_')[0]}/dt={fecha}/h=00/{match_id}.parquet"

    monkeypatch.setattr(poller, "subir_a_raw", subir)
    return subidas


def detalle(match_id):
    return {"metadata": {"matchId": match_id, "participants": [f"{match_id}-p{i}" for i in range(10)]},
            "info": {"participants": []}}


def revisados(conn):
    return {p for (p,) in conn.execute("SELECT puuid FROM puuids_conocidos WHERE fecha_ultimo_check IS NOT NULL")}


# ---------- descubrir ----------

def test_descubrir_encola_y_marca_revisado(conn, monkeypatch):
    db.guardar_puuid(conn, "a", "la1", "GOLD")
    db.guardar_puuid(conn, "b", "kr", "GOLD")
    monkeypatch.setattr(poller, "get_match_ids",
                        lambda puuid, region, count: {"a": ["LA1_1", "LA1_2"], "b": ["KR_1", "LA1_1"]}[puuid])

    assert poller.descubrir(conn, limite=10) == 3
    assert db.resumen_cola(conn) == {"pendiente": 3}
    assert revisados(conn) == {"a", "b"}


def test_descubrir_no_marca_revisado_si_riot_falla(conn, monkeypatch):
    db.guardar_puuid(conn, "a", "la1", "GOLD")

    def falla(puuid, region, count):
        raise RiotNoDisponibleError("503")

    monkeypatch.setattr(poller, "get_match_ids", falla)
    poller.descubrir(conn, limite=10)
    assert revisados(conn) == set()


def test_descubrir_se_detiene_con_key_invalida(conn, monkeypatch):
    db.guardar_puuid(conn, "a", "la1", "GOLD")

    def falla(puuid, region, count):
        raise KeyInvalidaError("401")

    monkeypatch.setattr(poller, "get_match_ids", falla)
    with pytest.raises(KeyInvalidaError):
        poller.descubrir(conn, limite=10)


# ---------- worker ----------

def test_worker_solo_toma_su_region_y_guarda_la_key(conn, monkeypatch, sin_s3):
    db.encolar(conn, ["VN2_1", "OC1_2", "LA1_3", "KR_4"])
    monkeypatch.setattr(poller, "get_match_detail", lambda match_id, region: detalle(match_id))

    assert poller.worker(conn, "sea") == 2
    assert sorted(sin_s3) == ["OC1_2", "VN2_1"]
    assert db.keys_registradas(conn)["VN2_1"].endswith("/VN2_1.parquet")
    assert {m for m, _ in db.obtener_pendientes(conn, ["la1", "kr"])} == {"LA1_3", "KR_4"}
    assert db.resumen_cola(conn) == {"subido": 2, "pendiente": 2}


def test_worker_descubre_los_participantes(conn, monkeypatch, sin_s3):
    db.encolar(conn, ["LA1_1"])
    monkeypatch.setattr(poller, "get_match_detail", lambda match_id, region: detalle(match_id))
    poller.worker(conn, "americas")
    assert conn.execute("SELECT count(*) FROM puuids_conocidos").fetchone()[0] == 10


def test_worker_404_es_error_definitivo(conn, monkeypatch, sin_s3):
    db.encolar(conn, ["LA1_1"])
    monkeypatch.setattr(poller, "get_match_detail", lambda match_id, region: None)
    poller.worker(conn, "americas")
    assert db.resumen_cola(conn) == {"error": 1}


def test_worker_se_detiene_si_riot_esta_caido_y_no_pierde_nada(conn, monkeypatch, sin_s3):
    ids = [f"VN2_{i}" for i in range(15)]
    db.encolar(conn, ids)

    def caido(match_id, region):
        raise RiotNoDisponibleError("503")

    monkeypatch.setattr(poller, "get_match_detail", caido)
    with pytest.raises(poller.RiotCaidoError):
        poller.worker(conn, "sea")
    # Los 10 que fallaron siguen pendientes (1 intento), los otros 5 ni se tocaron
    assert db.resumen_cola(conn) == {"pendiente": 15}


def test_worker_un_fallo_aislado_no_lo_detiene(conn, monkeypatch, sin_s3):
    db.encolar(conn, ["LA1_1", "LA1_2", "LA1_3"])

    def a_veces(match_id, region):
        if match_id == "LA1_2":
            raise RiotNoDisponibleError("503")
        return detalle(match_id)

    monkeypatch.setattr(poller, "get_match_detail", a_veces)
    assert poller.worker(conn, "americas") == 2
    assert db.resumen_cola(conn) == {"subido": 2, "pendiente": 1}


def test_worker_key_invalida_detiene(conn, monkeypatch, sin_s3):
    db.encolar(conn, ["LA1_1"])

    def falla(match_id, region):
        raise KeyInvalidaError("401")

    monkeypatch.setattr(poller, "get_match_detail", falla)
    with pytest.raises(KeyInvalidaError):
        poller.worker(conn, "americas")


def test_worker_respeta_el_limite(conn, monkeypatch, sin_s3):
    db.encolar(conn, [f"LA1_{i}" for i in range(5)])
    monkeypatch.setattr(poller, "get_match_detail", lambda match_id, region: detalle(match_id))
    assert poller.worker(conn, "americas", limite=2) == 2


# ---------- CLI ----------

def test_main_worker_regresa_codigo_2_si_riot_cae(conn, monkeypatch, sin_s3):
    db.encolar(conn, [f"VN2_{i}" for i in range(12)])

    def caido(match_id, region):
        raise RiotNoDisponibleError("503")

    monkeypatch.setattr(poller, "get_match_detail", caido)
    assert poller.main(["worker", "--region", "sea"]) == 2


def test_main_region_invalida():
    with pytest.raises(SystemExit):
        poller.main(["worker", "--region", "marte"])


def test_workers_en_paralelo_cada_region_con_su_conexion(conn, monkeypatch, sin_s3):
    ids = [f"LA1_{i}" for i in range(20)] + [f"EUW1_{i}" for i in range(20)] + [f"KR_{i}" for i in range(20)]
    db.encolar(conn, ids)
    monkeypatch.setattr(poller, "get_match_detail", lambda match_id, region: detalle(match_id))

    resultados = poller.workers_en_paralelo(["americas", "europe", "asia", "sea"])

    assert resultados == {"americas": 20, "europe": 20, "asia": 20, "sea": 0}
    assert db.resumen_cola(conn) == {"subido": 60}


def test_un_worker_caido_no_tumba_a_los_demas(conn, monkeypatch, sin_s3):
    db.encolar(conn, [f"LA1_{i}" for i in range(5)] + [f"VN2_{i}" for i in range(12)])

    def sea_caido(match_id, region):
        if region == "sea":
            raise RiotNoDisponibleError("503")
        return detalle(match_id)

    monkeypatch.setattr(poller, "get_match_detail", sea_caido)
    resultados = poller.workers_en_paralelo(["americas", "sea"])
    assert resultados == {"americas": 5, "sea": 0}
