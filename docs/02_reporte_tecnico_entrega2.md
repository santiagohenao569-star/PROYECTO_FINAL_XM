# Entrega 2 - Reporte tecnico del prototipo distribuido

## 1. Objetivo del prototipo

Validar que la infraestructura puede desplegarse de forma reproducible, almacenar informacion en HDFS, procesarla con Spark, separar registros segun reglas de calidad y producir datasets curados consultables.

## 2. Infraestructura implementada

El proyecto utiliza Docker Compose para desplegar:

- 1 NameNode;
- 2 DataNodes;
- 1 Spark Master;
- 2 Spark Workers;
- 1 Spark Client con JupyterLab;
- 1 broker Kafka disponible para experimentacion.

HDFS se configuro con replicacion 2. Spark accede a HDFS mediante `hdfs://namenode:9000`.

## 3. Ingesta

Se implemento ingesta batch desde la API de XM utilizando Python/requests. El periodo de trabajo es 2020-01-01 a 2026-11-19.

La ingesta divide el historico en ventanas y almacena la respuesta original en HDFS RAW. Tambien consulta los catalogos de embalses, rios y recursos.

La estrategia de reejecucion evita volver a descargar archivos ya presentes, lo que aporta idempotencia a nivel de archivos. Esta propiedad debe probarse formalmente antes de la entrega final.

## 4. Almacenamiento RAW

Se cargaron 8 metricas historicas y 3 catalogos. Para cada metrica historica se generaron 89 archivos, equivalentes a 712 archivos historicos; con los 3 catalogos se obtuvieron 715 archivos RAW.

Tamano logico aproximado observado por grupos:

| Grupo | Tamano aproximado |
|---|---:|
| `Gene` | 345,7 MB |
| `AporCaudal` | 8,9 MB |
| `AporCaudalMediHist` | 8,9 MB |
| `PorcApor` | 8,6 MB |
| `CapaUtilDiarMasa` | 5,9 MB |
| `VoluUtilDiarMasa` | 5,8 MB |
| `PorcVoluUtilDiar` | 5,6 MB |
| `PrecBolsNaci` | 1,6 MB |
| Catalogos | ~780 KB |

El almacenamiento fisico en HDFS es superior al tamano logico debido a la replicacion configurada en 2.

## 5. Volumen de eventos

El perfilamiento de `Gene` produjo:

- 89 archivos JSON RAW;
- 662.600 Items API;
- 15.902.400 posiciones horarias despues de normalizar 24 horas;
- 12.296.013 eventos con valor valido;
- 3.606.387 posiciones horarias sin valor.

Por lo tanto, la metrica `Gene` supera por si sola el minimo de 250.000 eventos.

## 6. RAW a CURATED

El job `spark/02_raw_a_curated.py` normaliza las estructuras diarias y horarias y escribe Parquet particionado por ano.

Resultados registrados:

| Metrica | Normalizados | Validos | Rechazados |
|---|---:|---:|---:|
| PorcVoluUtilDiar | 58.946 | 58.946 | 0 |
| VoluUtilDiarMasa | 58.946 | 58.946 | 0 |
| CapaUtilDiarMasa | 59.357 | 59.357 | 0 |
| AporCaudal | 97.419 | 97.419 | 0 |
| AporCaudalMediHist | 97.419 | 97.419 | 0 |
| PorcApor | 92.665 | 92.665 | 0 |
| Gene | 15.902.400 | 12.296.013 | 3.606.387 |
| PrecBolsNaci | 60.336 | 60.336 | 0 |

## 7. Reglas de calidad

Se implementaron:

1. `R1_COMPLETITUD`.
2. `R2_INTEGRIDAD_NUMERICA`.
3. `R3_RANGO`.
4. `R4_DUPLICADO`.

En `Gene`, los 3.606.387 rechazados observados fueron clasificados por R1_COMPLETITUD. Se trata de posiciones horarias sin valor. No se atribuye una causa sin evidencia adicional.

La concentracion de faltantes por ano observada fue:

| Ano | Validos | Sin valor | % sobre posiciones |
|---:|---:|---:|---:|
| 2020 | 1.419.818 | 141.190 | 9,04% |
| 2021 | 1.411.874 | 173.758 | 10,96% |
| 2022 | 1.469.857 | 228.071 | 13,43% |
| 2023 | 1.658.030 | 345.562 | 17,25% |
| 2024 | 1.861.593 | 567.927 | 23,38% |
| 2025 | 2.095.552 | 852.056 | 28,91% |
| 2026* | 2.379.289 | 1.297.823 | 35,29% |

`*` 2026 corresponde al periodo del proyecto hasta 2026-11-19.

**Interpretacion prudente:** la proporcion de posiciones sin valor aumenta en los periodos observados y no se distribuye de manera uniforme entre recursos. No se concluye deterioro de calidad ni una causa especifica sin controlar cambios en el numero de recursos, entradas/salidas del sistema, operacion o reglas de reporte.

## 8. Integridad del catalogo de recursos

`ListadoRecursos` contiene:

- 2.522 registros;
- 2.519 codigos distintos;
- 3 codigos duplicados.

Dos codigos presentaron duplicado exacto y un codigo (`PRG1`) presento dos variantes de nombre manteniendo iguales los atributos tecnicos observados.

Se creo un catalogo CURATED de una fila por codigo, conservando banderas y listas de valores para no ocultar la anomalia.

## 9. Integridad referencial Gene-catalogo

- 672 recursos distintos aparecen en `Gene`.
- 647 encuentran correspondencia en el catalogo actual.
- 25 no encuentran correspondencia.
- 160 recursos pueden clasificarse como hidraulicos segun `Type=HIDRAULICA` y `EnerSource=AGUA`.

La ausencia de codigos no se clasifica automaticamente como error debido a que el catalogo es vigente y la generacion es historica.

## 10. Enriquecimiento de generacion

Se realizo un LEFT JOIN entre `Gene` CURATED y el catalogo CURATED.

Resultados:

- filas antes: 12.296.013;
- filas despues: 12.296.013;
- eventos sin catalogo: 112.246;
- recursos sin catalogo: 25;
- eventos hidraulicos: 7.277.050;
- recursos hidraulicos: 160.

La igualdad de conteos antes/despues valida que el JOIN no produjo multiplicacion de registros.

## 11. Estado de requisitos pendientes de esta entrega

Todavia deben completarse/documentarse:

- materializacion de al menos una salida ANALYTICS;
- dos consultas de negocio y una consulta tecnica formalmente presentadas;
- evidencia de plan/stages/shuffle en Spark UI;
- tiempos de ejecucion y tamano procesado;
- verificacion con subconjunto de resultado conocido;
- segunda fuente de tipo distinto.

## 12. Limitaciones actuales

- El catalogo de recursos no es una dimension historica.
- Las unidades de cada metrica deben validarse documentalmente antes de interpretacion cuantitativa final.
- El cluster distribuido se ejecuta sobre un unico host fisico en el entorno academico.
- El volumen actual no implica por si solo que un equipo moderno sea incapaz de procesarlo; el valor del prototipo esta en reproducibilidad, distribucion, escalamiento y observabilidad.
