#!/bin/bash
# get_matches.sh [puuids.txt | puuid] [salida.txt] [count] [queue]
# Extrae los ultimos match IDs ranked de uno o varios PUUID.

INPUT="${1:-puuids.txt}"
OUTPUT="${2:-match_ids.txt}"
COUNT="${3:-20}"
QUEUE="${4:-420}"   # SoloQ=420, Flex=440
REGION="americas"

if [[ -z "${RIOT_API_KEY:-}" ]]; then
  echo "Falta RIOT_API_KEY. Haz: export RIOT_API_KEY=RGAPI-..." >&2
  exit 1
fi

# Si es un archivo lee todos los puuids, si no lo trata como un solo puuid.
if [[ -f "$INPUT" ]]; then
  LISTA=$(cat "$INPUT")
else
  LISTA="$INPUT"
fi

TMP=$(mktemp)
trap 'rm -f "$TMP"' EXIT

while read -r puuid; do
  [[ -z "$puuid" ]] && continue
  curl -sf -H "X-Riot-Token: $RIOT_API_KEY" "https://${REGION}.api.riotgames.com/lol/match/v5/matches/by-puuid/${puuid}/ids?queue=${QUEUE}&start=0&count=${COUNT}" | jq -r '.[]' >> "$TMP"
  sleep 1.2
done <<< "$LISTA"

if [[ -f "$OUTPUT" ]]; then
  cat "$OUTPUT" "$TMP" | sort -u > "${OUTPUT}.new"
else
  sort -u "$TMP" > "${OUTPUT}.new"
fi
mv "${OUTPUT}.new" "$OUTPUT"

echo "Listo: $(wc -l < "$OUTPUT") match IDs unicos en $OUTPUT"
