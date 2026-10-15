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


def ligas(conn):
    return dict(conn.execute("SELECT puuid, liga FROM puuids_conocidos").fetchall())


def test_sin_rango_queda_unranked_y_no_se_repite(conn, monkeypatch):
    for p in ["a", "b", "c"]:
        db.guardar_puuid(conn, p, "la1", None)
    rangos = {"a": "GOLD", "b": None, "c": "DIAMOND"}
    consultados = []

    def rango_falso(puuid, platform):
        consultados.append(puuid)
        return rangos[puuid]

    monkeypatch.setattr(poller, "get_rank_by_puuid", rango_falso)

    assert poller.actualizar_rangos_pendientes(conn, limite=10) == 3
    assert ligas(conn) == {"a": "GOLD", "b": "UNRANKED", "c": "DIAMOND"}

    consultados.clear()
    assert poller.actualizar_rangos_pendientes(conn, limite=10) == 0
    assert consultados == []


def test_avanza_por_tandas(conn, monkeypatch):
    for i in range(5):
        db.guardar_puuid(conn, f"p{i}", "la1", None)
    monkeypatch.setattr(poller, "get_rank_by_puuid", lambda puuid, platform: None)

    poller.actualizar_rangos_pendientes(conn, limite=2)
    poller.actualizar_rangos_pendientes(conn, limite=2)
    poller.actualizar_rangos_pendientes(conn, limite=2)
    assert all(v == "UNRANKED" for v in ligas(conn).values())


def test_error_temporal_no_detiene_la_tanda(conn, monkeypatch):
    for p in ["a", "b"]:
        db.guardar_puuid(conn, p, "la1", None)

    def rango_falso(puuid, platform):
        if puuid == "a":
            raise RiotNoDisponibleError("503")
        return "GOLD"

    monkeypatch.setattr(poller, "get_rank_by_puuid", rango_falso)
    assert poller.actualizar_rangos_pendientes(conn, limite=10) == 1
    assert ligas(conn) == {"a": None, "b": "GOLD"}


def test_key_invalida_detiene(conn, monkeypatch):
    db.guardar_puuid(conn, "a", "la1", None)

    def rango_falso(puuid, platform):
        raise KeyInvalidaError("401")

    monkeypatch.setattr(poller, "get_rank_by_puuid", rango_falso)
    with pytest.raises(KeyInvalidaError):
        poller.actualizar_rangos_pendientes(conn, limite=10)


def test_divisiones_incluyen_esmeralda():
    tiers = {tier for _, tier, _ in poller.DIVISIONES}
    assert tiers == {"DIAMOND", "EMERALD", "PLATINUM"}
