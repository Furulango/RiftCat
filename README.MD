**ADD KITTYMETALOL in the future

**NOTA: por que el poller es secuencial y no paralelo**
El rate limit de Riot (~0.8 req/s) es compartido por toda la app, sin
importar cuantos hilos hagan las llamadas. Paralelizar no aumenta el
throughput real, solo choca con el 429 mas rapido. El paralelismo si
tendria sentido en las partes que no dependen de Riot (empaquetar/subir
a S3 en paralelo), pero el cuello de botella esta en la descarga, no en
el procesamiento local. Escalar el volumen es cosa de tiempo (correr el
poller de forma continua via cron/EventBridge), no de mas keys o hilos.
