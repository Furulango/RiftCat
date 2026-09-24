**ADD KITTYMETALOL in the future

**NOTA: por que el poller es secuencial y no paralelo**
El rate limit de Riot (~0.8 req/s) es compartido por toda la app, sin
importar cuantos hilos hagan las llamadas. Paralelizar no aumenta el
throughput real, solo choca con el 429 mas rapido. El paralelismo si
tendria sentido en las partes que no dependen de Riot (empaquetar/subir
a S3 en paralelo), pero el cuello de botella esta en la descarga, no en
el procesamiento local. Escalar el volumen es cosa de tiempo (correr el
poller de forma continua via cron/EventBridge), no de mas keys o hilos.


**NOTA: por que el particionado por hash, aunque se vea "vacio" ahorita**
Con pocas partidas, el hash de 2 digitos (256 combinaciones posibles)
genera casi una carpeta por archivo, lo cual se ve raro navegando MinIO
a mano. Es esperado: el proposito del hash no es verse ordenado a bajo
volumen, es evitar cuellos de botella de escritura en S3 cuando el
volumen crezca a cientos de miles de archivos. El costo de tenerlo desde
el inicio es minimo (unas 4 lineas de codigo), mientras que agregarlo
despues, con datos ya escritos sin particionar, seria mucho mas caro.
Las queries (DuckDB/Athena) leen con un patron glob que ignora cuantas
subcarpetas haya, asi que no afecta el performance de lectura, solo la
navegacion visual manual.


**NOTA: por que el rango es un snapshot, no un valor que se actualiza solo**
La API de Riot no expone historial de rango por fecha, solo el rango
actual al momento de preguntar. Un jugador capturado como CHALLENGER
puede bajar de rango dias despues sin que la tabla se entere (caso real
confirmado: un jugador de la muestra inicial ya aparecia como
Grandmaster en su perfil web pocos dias despues de capturarlo como
Challenger). Se acepta como aproximacion razonable para el objetivo de
"mejores campeones por liga", ya que el rango de alto nivel no cambia
drasticamente en pocos dias. Pendiente: mecanismo de refresco periodico
(la columna fecha_consultado ya existe para eso, aunque hoy no se usa
para refrescar, solo para saber cuando se consulto por primera vez).

**NOTA: por que el snowball sampling es gradual, no recursivo en la misma corrida**
Procesar de inmediato las partidas de cada jugador recien descubierto
puede crecer sin control (10 jugadores nuevos por partida, cada uno
trayendo mas partidas con mas jugadores). En vez de eso, cada partida
procesada solo guarda los puuids de sus 10 participantes sin rango
todavia (liga NULL); sus partidas se recogen en una corrida futura del
poller. Una tarea aparte, al final de cada corrida, actualiza hasta N
rangos pendientes por vez. Esto vuelve el crecimiento del dataset
predecible y apto para correr sin supervision via cron.

**NOTA: por que la consulta de rangos vive en el poller y no en db.py**
db.py solo habla con SQLite (una responsabilidad). La funcion que
actualiza rangos pendientes necesita hacer una llamada HTTP a la API de
Riot ademas de tocar la base de datos, asi que se dejo en poller.py
(el orquestador), manteniendo la separacion que ya seguia el resto del
proyecto: db.py = solo SQLite, riot_client.py = solo HTTP, poller.py =
junta ambos.
