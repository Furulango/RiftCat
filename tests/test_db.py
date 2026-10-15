import sqlite3

import pytest

from core import db


@pytest.fixture
def conn(tmp_path, monkeypatch):
    monkeypatch.setattr(db, "DB_PATH", str(tmp_path / "test.db"))
    c = db.get_conn()
    yield c
    c.close()


# ---------- cola ----------

def test_encolar_agrega_y_no_duplica(conn):
    assert db.encolar(conn, ["LA1_1", "KR_2"]) == 2
    assert db.encolar(conn, ["LA1_1", "KR_2", "NA1_3"]) == 1
    assert db.resumen_cola(conn) == {"pendiente": 3}


def test_encolar_ignora_lo_que_ya_esta_en_raw(conn):
    db.marcar_visto(conn, "LA1_1")
    assert db.encolar(conn, ["LA1_1", "LA1_2"]) == 1


def test_obtener_pendientes_filtra_por_plataforma(conn):
    db.encolar(conn, ["LA1_1", "NA1_2", "KR_3"])
    assert {m for m, _ in db.obtener_pendientes(conn, ["la1", "na1"])} == {"LA1_1", "NA1_2"}


def test_subido_sale_de_pendientes(conn):
    db.encolar(conn, ["LA1_1"])
    db.marcar_subido(conn, "LA1_1")
    assert db.obtener_pendientes(conn, ["la1"]) == []
    assert db.resumen_cola(conn) == {"subido": 1}


def test_fallo_reintenta_hasta_el_maximo(conn):
    db.encolar(conn, ["LA1_1"])
    for _ in range(db.MAX_INTENTOS - 1):
        db.marcar_fallo(conn, "LA1_1", "503")
        assert db.obtener_pendientes(conn, ["la1"]) != []
    db.marcar_fallo(conn, "LA1_1", "503")
    assert db.obtener_pendientes(conn, ["la1"]) == []
    assert db.resumen_cola(conn) == {"error": 1}


# ---------- s3_key y pendientes de transformar ----------

def test_marcar_visto_guarda_key_y_no_la_borra(conn):
    db.marcar_visto(conn, "LA1_1", "match_v5/platform=LA1/x/LA1_1.parquet")
    db.marcar_visto(conn, "LA1_1")
    assert db.keys_registradas(conn) == {"LA1_1": "match_v5/platform=LA1/x/LA1_1.parquet"}


def test_guardar_s3_keys_solo_llena_vacias(conn):
    db.marcar_visto(conn, "LA1_1")
    db.marcar_visto(conn, "LA1_2", "key_original")
    llenadas = db.guardar_s3_keys(conn, {"LA1_1": "k1", "LA1_2": "otra", "LA1_9": "no_registrada"})
    assert llenadas == 1
    assert db.keys_registradas(conn) == {"LA1_1": "k1", "LA1_2": "key_original"}


def test_pendientes_de_transformar(conn):
    db.marcar_visto(conn, "LA1_1", "k1")
    db.marcar_visto(conn, "LA1_2", "k2")
    db.marcar_visto(conn, "LA1_3")
    db.marcar_transformado(conn, "LA1_2")
    assert sorted(db.pendientes_de_transformar(conn)) == [("LA1_1", "k1"), ("LA1_3", None)]


def test_migra_base_vieja_sin_s3_key(tmp_path, monkeypatch):
    ruta = tmp_path / "vieja.db"
    vieja = sqlite3.connect(ruta)
    vieja.execute("CREATE TABLE matches_vistos (match_id TEXT PRIMARY KEY, fecha_procesado TEXT)")
    vieja.execute("INSERT INTO matches_vistos VALUES ('LA1_1', '2026-09-28')")
    vieja.execute("CREATE TABLE puuids_conocidos (puuid TEXT PRIMARY KEY, region TEXT, liga TEXT, fecha_consultado TEXT)")
    vieja.commit()
    vieja.close()

    monkeypatch.setattr(db, "DB_PATH", str(ruta))
    conn = db.get_conn()
    assert db.keys_registradas(conn) == {"LA1_1": None}
    assert db.pendientes_de_transformar(conn) == [("LA1_1", None)]
    conn.close()
