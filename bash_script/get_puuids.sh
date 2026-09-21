#!/bin/bash
# get_puuids.sh [puuids.txt] [platform]
# Extrae los puuids de Challenger (RANKED_SOLO_5x5) y los guarda uno por linea.

OUTPUT="${1:-puuids.txt}"
PLATFORM="${2:-la1}"   # league-v4 usa routing por plataforma, no regional

if [[ -z "${RIOT_API_KEY:-}" ]]; then
  echo "Falta RIOT_API_KEY. Haz: export RIOT_API_KEY=RGAPI-..." >&2
  exit 1
fi

TMP=$(mktemp)
trap 'rm -f "$TMP"' EXIT

curl -sf -H "X-Riot-Token: $RIOT_API_KEY" "https://${PLATFORM}.api.riotgames.com/lol/league/v4/challengerleagues/by-queue/RANKED_SOLO_5x5" | jq -r '.entries[].puuid' > "$TMP"

if [[ ! -s "$TMP" ]]; then
  echo "No llegaron puuids. Revisa la key (expira cada 24 h) o el host." >&2
  exit 1
fi

if [[ -f "$OUTPUT" ]]; then
  cat "$OUTPUT" "$TMP" | sort -u > "${OUTPUT}.new"
else
  sort -u "$TMP" > "${OUTPUT}.new"
fi
mv "${OUTPUT}.new" "$OUTPUT"

echo "Listo: $(wc -l < "$OUTPUT") puuids en $OUTPUT"
