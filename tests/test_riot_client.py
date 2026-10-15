import pytest
import requests

from core import riot_client
from core.riot_client import KeyInvalidaError, RiotNoDisponibleError


class RespuestaFalsa:
    def __init__(self, status_code, json_data=None, headers=None):
        self.status_code = status_code
        self._json = json_data
        self.headers = headers or {}

    def json(self):
        return self._json


@pytest.fixture
def sin_esperas(monkeypatch):
    esperas = []
    monkeypatch.setattr(riot_client.time, "sleep", esperas.append)
    return esperas


def respuestas(monkeypatch, secuencia):
    """Hace que requests.get devuelva (o lance) los elementos de la secuencia en orden."""
    llamadas = []

    def get_falso(url, headers=None, timeout=None):
        llamadas.append(timeout)
        r = secuencia.pop(0)
        if isinstance(r, Exception):
            raise r
        return r

    monkeypatch.setattr(riot_client.requests, "get", get_falso)
    return llamadas


def test_200_devuelve_json(monkeypatch, sin_esperas):
    llamadas = respuestas(monkeypatch, [RespuestaFalsa(200, {"ok": 1})])
    assert riot_client.get_con_reintento("u") == {"ok": 1}
    assert llamadas == [riot_client.TIMEOUT]


def test_404_devuelve_none(monkeypatch, sin_esperas):
    respuestas(monkeypatch, [RespuestaFalsa(404)])
    assert riot_client.get_con_reintento("u") is None


@pytest.mark.parametrize("codigo", [401, 403])
def test_401_403_lanzan_key_invalida_sin_reintentar(monkeypatch, sin_esperas, codigo):
    llamadas = respuestas(monkeypatch, [RespuestaFalsa(codigo)])
    with pytest.raises(KeyInvalidaError):
        riot_client.get_con_reintento("u")
    assert len(llamadas) == 1


def test_503_se_reintenta_y_luego_funciona(monkeypatch, sin_esperas):
    respuestas(monkeypatch, [RespuestaFalsa(503), RespuestaFalsa(503), RespuestaFalsa(200, [1])])
    assert riot_client.get_con_reintento("u") == [1]
    assert sin_esperas == [2, 4]


def test_503_persistente_lanza_no_disponible(monkeypatch, sin_esperas):
    respuestas(monkeypatch, [RespuestaFalsa(503)] * 5)
    with pytest.raises(RiotNoDisponibleError):
        riot_client.get_con_reintento("u", max_intentos=5)
    assert sin_esperas == [2, 4, 8, 16, 32]


def test_429_respeta_retry_after(monkeypatch, sin_esperas):
    respuestas(monkeypatch, [RespuestaFalsa(429, headers={"Retry-After": "7"}), RespuestaFalsa(200, {})])
    assert riot_client.get_con_reintento("u") == {}
    assert sin_esperas == [7]


def test_timeout_de_red_se_reintenta(monkeypatch, sin_esperas):
    respuestas(monkeypatch, [requests.Timeout(), RespuestaFalsa(200, {"ok": 1})])
    assert riot_client.get_con_reintento("u") == {"ok": 1}


def test_un_401_dentro_del_match_id_no_es_error_de_key(monkeypatch, sin_esperas):
    # El bug del 29-09-26: VN2_1592054010 contiene "401" en el texto
    respuestas(monkeypatch, [RespuestaFalsa(503)] * 2)
    with pytest.raises(RiotNoDisponibleError):
        riot_client.get_con_reintento("https://sea.api.riotgames.com/lol/match/v5/matches/VN2_1592054010", max_intentos=2)


def test_sin_api_key_lanza_key_invalida(monkeypatch):
    monkeypatch.delenv("RIOT_API_KEY")
    with pytest.raises(KeyInvalidaError):
        riot_client.get_con_reintento("u")


def test_region_de_plataforma():
    assert riot_client.region_de_plataforma("LA1") == "americas"
    assert riot_client.region_de_plataforma("me1") == "europe"
    with pytest.raises(ValueError):
        riot_client.region_de_plataforma("xx9")


def test_plataformas_de_region():
    assert set(riot_client.plataformas_de_region("sea")) == {"sg2", "tw2", "vn2", "oc1"}
