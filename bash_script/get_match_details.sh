#!/bin/bash
# get_match_details.sh <match_id> [carpeta_salida]
# Extrae la informacion de la partida y la guarda en un json.

MATCH_ID="$1"
OUTDIR="${2:-matches}"
REGION="americas"

if [[ -z "$MATCH_ID" ]]; then
  echo "Uso: $0 <match_id> [carpeta_salida]" >&2
  exit 1
fi

if [[ -z "${RIOT_API_KEY:-}" ]]; then
  echo "Falta RIOT_API_KEY. Haz: export RIOT_API_KEY=RGAPI-..." >&2
  exit 1
fi

mkdir -p "$OUTDIR"

HTTP_CODE=$(curl -s -w '%{http_code}' -H "X-Riot-Token: $RIOT_API_KEY" \
  "https://${REGION}.api.riotgames.com/lol/match/v5/matches/${MATCH_ID}" \
  -o "${OUTDIR}/${MATCH_ID}.json")

if [[ "$HTTP_CODE" != "200" ]]; then
  echo "Error $HTTP_CODE al pedir $MATCH_ID" >&2
  rm -f "${OUTDIR}/${MATCH_ID}.json"
  exit 1
fi

echo "Guardado: ${OUTDIR}/${MATCH_ID}.json"
