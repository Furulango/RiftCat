# RiftCat: fases, definición de terminado y estado

Las fechas vienen del Study Plan v6.0 (5 Ago 2026) y pueden moverse. Si algo cambió, manda lo que diga la sección "Estado actual".

## Versión 0 local: prototipo en mi máquina (antes de tocar AWS)
Objetivo: validar la lógica del pipeline sin costo, con equivalentes locales de cada servicio (ver la tabla en el archivo de arquitectura).
* Poller de Python que consume la API de Riot con manejo de 429 y backoff.
* Stream local (Redpanda o Kafka en Docker) y consumer que escribe Parquet por lotes. **Decisión tomada:** se omite por ahora — el poller escribe Parquet directo a `raw`, sin stream intermedio. Pendiente documentar formalmente el porqué antes de cerrar la Versión 0.
* MinIO como S3 local, con particionado por fecha y sub prefijo hash. **Hecho**, y se agregó un nivel adicional de particionado por `platform=` (servidor de Riot) para permitir segmentación futura por región/servidor.
* DuckDB consultando el Parquet (equivalente a Athena). **Hecho** para la capa `raw` (vía `httpfs` + `CREATE SECRET` contra MinIO).
* Todo configurable por variables de entorno. **Hecho** (`~/riftcat-env.sh`: Riot API key, credenciales de MinIO/AWS, keyring backend).
Terminado cuando: el pipeline corre de punta a punta en local, el código no tiene nada específico de mi máquina, y existe la tabla de equivalencias local ↔ AWS en el README.
Fecha de salida hacia cloud: (completar). Esta fase tiene fecha límite para no quedarme puliendo en local mientras el plan pide aplicar a trabajos.

Checklist de transferencia a cloud:
* Cambiar `endpoint_url` y credenciales, y confirmar que el código corre contra S3 real sin más cambios.
* Empaquetar el poller como Lambda y programarlo con EventBridge.
* Reemplazar el stream local por Kinesis y el consumer por Firehose (si se decide incluir stream).
* Reemplazar DuckDB por Athena sobre Glue Data Catalog.
* Volver a medir latencia y costo con datos reales en AWS.

## Versión 0 en cloud: ingesta y consulta (semanas 1 a 6 del plan, hasta 13 Sep)
Objetivo: pipeline funcional de punta a punta.
* Primer stream de Kinesis de prueba.
* S3 con particionado por fecha y Firehose hacia S3.
* Athena consultando datos reales de la API de Riot.
* v0.1: Kinesis → S3 → Athena funcionando end to end.
* README con métricas reales.
Terminado cuando: alguien puede clonar el repo, seguir el README y ver datos reales consultables en Athena.

## Versión 1: producción con Terraform (semanas 12 a 16, 26 Oct a 22 Nov)
Objetivo: todo reproducible como código y con calidad de producción.
* Migrar la infraestructura existente a Terraform (kinesis, s3, athena, iam).
* GitHub Actions: terraform plan en cada PR, apply en merge.
* Dashboards de CloudWatch y validación con checkov.
* README con métricas reales: p99 de latencia, costo por GB, eventos por segundo.
Terminado cuando: se puede destruir y recrear el stack completo con un comando y el CI lo valida.

## Versión 2: capa de ML (semanas 17 a 21, 7 Dic a 24 Ene)
Objetivo: pipeline de ML real sobre los datos.
* Feature store con S3 y Glue Data Catalog.
* SageMaker Training Pipeline para el modelo de win prediction.
* Model serving (Real Time Endpoint o Lambda batch).
* Model monitoring con CloudWatch y Model Monitor.
* Case study para LinkedIn.
Terminado cuando: hay un modelo entrenado, servido y monitoreado, y el case study publicado.

## Relación con el resto del plan
* Los jueves son el bloque semanal de avance de infra de RiftCat.
* Del Día 5 del roadmap de Linux en adelante, lo que se practica se aplica directo en RiftCat.
* Si llega una oferta de trabajo, la oferta gana sobre el plan. RiftCat se puede seguir puliendo ya empleado.

## Estado actual (actualizar en cada sesión importante)
* Fecha de esta actualización: 15 Oct 2026
* Infra desplegada: 100% local en el servidor B (MinIO en Docker, SQLite, DuckDB). Nada en AWS real todavía.
* Versión en la que estoy: Versión 0 local, etapa de cierre (nivel 1 del modelo de datos). Atraso de ~5 semanas contra el plan (V0 cloud debía cerrar el 13 Sep; V1 empieza el 26 Oct).
* Estado de datos (cerrado el 15 Oct): 147,783 partidas en `raw`, 147,770 en curated (1,477,700 filas, 10 por partida, sin duplicados) más 13 partidas vacías marcadas. `reconciliar` confirma que SQLite y raw coinciden al 100%. Quedan 6,064 partidas de sea en `match_queue` como pendientes: se decidió no forzarlas; las tomará el worker de sea del poller nuevo. Solo 26,440 puuids de ~860K tienen liga conocida.
* Último avance (15 Oct):
  - Bugs arreglados: requirements.txt reproducible; riot_client con timeout, reintentos con backoff para 429/5xx/red y errores tipados (KeyInvalidaError por código 401/403 real, RiotNoDisponibleError); rangos pendientes ya no se atoran (orden por antigüedad, UNRANKED); Esmeralda en DIVISIONES; query.py y reset_completo.sh corregidos; config.py eliminado.
  - SQLite como fuente de verdad: `match_queue` y `s3_key` en `matches_vistos`; el transform ya no lista raw (de ~380 s a instantáneo). `core/mantenimiento.py` con `rellenar-keys` y `reconciliar` (ListObjects, ~15-20 s para 147K archivos).
  - Partidas sin participantes se marcan y se reportan en vez de reintentarse.
  - 32 tests con pytest. `MODELO_DE_DATOS.md` en el repo con los 5 niveles y el checklist del nivel 1.
* Avance anterior (28 a 30 Sep): transform incremental y por lotes con lectura por rutas exactas (40+ min a ~10 s por lote), esquema explícito, cola persistente tras casi perder ~62K match_ids, corrida dirigida a 9 servidores estancados (~8K partidas cada uno), workers por región, Personal API Key.
* Hallazgos de diseño:
  - El rate limit es por routing value: por plataforma en League-V4 (15 contadores) y por región en Match-V5 (4 contadores). Para descargar partidas el máximo paralelismo útil es 4 workers.
  - Riot no hace matchmaking entre regiones: el snowball nunca cruza servidores, y la cola por rowid deja servidores estancados.
  - Balance: ponderar por servidor en el análisis (post-estratificación) en vez de perseguir balance perfecto en la ingesta.
  - Quien escribe registra lo que escribió; listar el data lake en cada corrida no escala. En AWS: S3 Event Notifications a SQS e Iceberg para curated.
* Bloqueado por: nada.
* Siguiente paso concreto:
  1. Poller en dos comandos: `descubrir` (encola en match_queue) y `worker --region` (consume la cola), sin `python -c`.
  2. README de presentación: qué es, cómo correrlo, diagrama, tabla local ↔ AWS, decisión de omitir el stream.
  3. Con eso se cierra el nivel 1 y la V0 local. Salir a AWS (S3, DynamoDB, Lambda + EventBridge, Glue + Athena) y arrancar la V1 con Terraform.
  Opcionales: corrida dirigida a la1 (~1.5K partidas), snapshot de rango para los puuids sin liga, nivel 2 del modelo de datos.
