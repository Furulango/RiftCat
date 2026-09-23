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
