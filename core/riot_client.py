import os
import time

import requests

from core.logger_config import get_logger

logger = get_logger(__name__)

TIMEOUT = 10
ESPERA_BASE_5XX = 2
ESPERA_MAXIMA = 60
ESTADOS_TEMPORALES = {500, 502, 503, 504}

RUTEO_REGIONAL = {
    "na1": "americas", "br1": "americas", "la1": "americas", "la2": "americas",
    "euw1": "europe", "eun1": "europe", "tr1": "europe", "ru": "europe", "me1": "europe",
    "kr": "asia", "jp1": "asia",
    "sg2": "sea", "tw2": "sea", "vn2": "sea", "oc1": "sea",
}


class RiotError(Exception):
    """Error base de la API de Riot."""


class KeyInvalidaError(RiotError):
    """401 o 403: la key falta, expiro o no tiene permiso. Reintentar no sirve."""


class RiotNoDisponibleError(RiotError):
    """429 o 5xx que siguieron fallando despues de todos los reintentos."""


def region_de_plataforma(platform):
    platform = platform.lower()
    if platform not in RUTEO_REGIONAL:
        raise ValueError(f"Plataforma desconocida: {platform}")
    return RUTEO_REGIONAL[platform]


def plataformas_de_region(region):
    return [p for p, r in RUTEO_REGIONAL.items() if r == region]


def _headers():
    api_key = os.environ.get("RIOT_API_KEY")
    if not api_key:
        raise KeyInvalidaError("RIOT_API_KEY no esta definida")
    return {"X-Riot-Token": api_key}


def _espera_5xx(intento):
    return min(ESPERA_BASE_5XX * 2 ** (intento - 1), ESPERA_MAXIMA)


def get_con_reintento(url, max_intentos=5):
    headers = _headers()
    ultimo_problema = None

    for intento in range(1, max_intentos + 1):
        try:
            respuesta = requests.get(url, headers=headers, timeout=TIMEOUT)
        except (requests.ConnectionError, requests.Timeout) as e:
            ultimo_problema = f"sin respuesta ({type(e).__name__})"
            espera = _espera_5xx(intento)
            logger.warning(f"{ultimo_problema}, esperando {espera}s (intento {intento}/{max_intentos}): {url}")
            time.sleep(espera)
            continue

        codigo = respuesta.status_code

        if codigo == 200:
            return respuesta.json()

        if codigo == 404:
            logger.warning(f"404: no encontrado en {url}")
            return None

        if codigo in (401, 403):
            raise KeyInvalidaError(f"{codigo} en {url}")

        if codigo == 429:
            espera = int(respuesta.headers.get("Retry-After", _espera_5xx(intento)))
            ultimo_problema = "429"
            logger.warning(f"429, esperando {espera}s (intento {intento}/{max_intentos}): {url}")
            time.sleep(espera)
            continue

        if codigo in ESTADOS_TEMPORALES:
            espera = _espera_5xx(intento)
            ultimo_problema = str(codigo)
            logger.warning(f"{codigo}, esperando {espera}s (intento {intento}/{max_intentos}): {url}")
            time.sleep(espera)
            continue

        raise RiotError(f"{codigo} inesperado en {url}")

    raise RiotNoDisponibleError(f"{ultimo_problema} tras {max_intentos} intentos en {url}")


def get_puuids_challenger(platform="la1"):
    url = f"https://{platform}.api.riotgames.com/lol/league/v4/challengerleagues/by-queue/RANKED_SOLO_5x5"
    data = get_con_reintento(url)
    return [entry["puuid"] for entry in data["entries"]] if data else []


def get_match_ids(puuid, region="americas", queue=420, count=20):
    url = (
        f"https://{region}.api.riotgames.com/lol/match/v5/matches/by-puuid/{puuid}/ids"
        f"?queue={queue}&start=0&count={count}"
    )
    return get_con_reintento(url) or []


def get_match_detail(match_id, region="americas"):
    url = f"https://{region}.api.riotgames.com/lol/match/v5/matches/{match_id}"
    return get_con_reintento(url)


def get_rank_by_puuid(puuid, platform="la1"):
    url = f"https://{platform}.api.riotgames.com/lol/league/v4/entries/by-puuid/{puuid}"
    data = get_con_reintento(url)
    if not data:
        return None

    for entrada in data:
        if entrada["queueType"] == "RANKED_SOLO_5x5":
            return entrada["tier"]

    return None


def get_puuids_by_tier(queue, tier, division, platform="la1"):
    url = f"https://{platform}.api.riotgames.com/lol/league/v4/entries/{queue}/{tier}/{division}"
    data = get_con_reintento(url)
    if not data:
        return []
    return [entrada["puuid"] for entrada in data]
