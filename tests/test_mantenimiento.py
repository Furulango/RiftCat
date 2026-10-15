from core.mantenimiento import comparar


def test_comparar_detecta_cada_diferencia():
    en_raw = {"A": "k/A", "B": "k/B", "C": "k/C", "D": "k/D"}
    en_sqlite = {"A": "k/A", "B": None, "D": "k/OTRA", "E": "k/E"}
    r = comparar(en_raw, en_sqlite)
    assert r == {
        "en_raw_sin_registro": ["C"],
        "registradas_sin_archivo": ["E"],
        "sin_key": ["B"],
        "key_distinta": ["D"],
    }


def test_comparar_todo_coincide():
    datos = {"A": "k/A"}
    assert all(not v for v in comparar(datos, dict(datos)).values())
