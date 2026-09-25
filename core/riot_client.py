import time
import os
import requests

API_KEY = os.environ.get("RIOT_API_KEY")

if not API_KEY:
    raise ValueError("RIOT_API_KEY environment variable is not set.")


def get_con_reintento(url, max_intentos=5):
    headers = {"X-Riot-Token": API_KEY}

    for intento in range(1, max_intentos + 1):
        respuesta = requests.get(url, headers=headers)

        if respuesta.status_code == 200:
            return respuesta.json()

        if respuesta.status_code == 429:
            espera = int(respuesta.headers.get("Retry-After", 1))
            print(f"429 recibido, esperando {espera}s (intento {intento}/{max_intentos})")
            time.sleep(espera)
            continue

        if respuesta.status_code == 404:
            print(f"404: no encontrado en {url}")
            return None

        respuesta.raise_for_status()

    raise RuntimeError(f"Se agotaron los {max_intentos} intentos por 429 en {url}")

def get_puuids_challenger(platform="la1"):
    url = f"https://{platform}.api.riotgames.com/lol/league/v4/challengerleagues/by-queue/RANKED_SOLO_5x5"
    data = get_con_reintento(url)
    return [entry["puuid"] for entry in data["entries"]]

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
