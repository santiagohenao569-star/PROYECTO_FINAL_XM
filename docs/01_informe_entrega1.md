# Entrega 1 - Diagnostico y arquitectura inicial

## 1. Contexto

El proyecto se desarrolla en el contexto del sector electrico colombiano y utiliza informacion publica de XM. El interes principal es estructurar una plataforma de datos que permita analizar la evolucion del recurso hidrico y relacionarla, de manera descriptiva y no causal, con variables del sistema como la generacion electrica y el precio de bolsa.

El usuario objetivo del prototipo es un analista que necesite consultar periodos amplios de informacion historica, verificar la calidad de los datos y responder preguntas sobre embalses, aportes hidricos, generacion y precios sin depender de una preparacion manual diferente para cada consulta.

## 2. Problema

La informacion requerida se encuentra distribuida entre diferentes metricas y estructuras de la API. Las metricas diarias y horarias no comparten exactamente la misma estructura, el volumen crece al ampliar el periodo y para algunas consultas es necesario cruzar series historicas con catalogos de entidades.

El problema tecnico consiste en construir un flujo reproducible que conserve el dato original, normalice las estructuras, controle calidad, permita procesamiento distribuido y produzca datos consultables para las preguntas priorizadas.

## 3. Alcance

Periodo historico configurado: **2020-01-01 a 2026-11-19**.

El alcance incluye:

- ingesta batch desde XM;
- almacenamiento RAW en HDFS;
- normalizacion y calidad mediante Spark;
- persistencia CURATED en Parquet;
- catalogos de embalses, rios y recursos;
- enriquecimiento de generacion con el catalogo de recursos;
- consultas analiticas mediante Jupyter/Spark SQL;
- resultados finales en una zona ANALYTICS.

No se pretende construir una plataforma productiva de alta disponibilidad ni demostrar causalidad economica/hidrologica.

## 4. Restricciones y criterios de exito

Restricciones:

- infraestructura ejecutada en computadores de laboratorio mediante Docker;
- dependencia de disponibilidad y estructura de la API publica;
- catalogos descargados como fotografias vigentes, no necesariamente historicas;
- recursos computacionales limitados frente a un cluster de produccion.

Criterios de exito:

- reproducir la infraestructura desde archivos versionados;
- cargar y procesar mas de 250.000 eventos;
- conservar RAW y generar CURATED;
- ejecutar reglas de calidad con trazabilidad de rechazados;
- responder al menos cinco consultas finales;
- demostrar al menos una operacion distribuida con evidencia de Spark;
- documentar limitaciones, desempeno, escalamiento y recuperacion.

## 5. Preguntas priorizadas

1. ¿Que porcentaje de registros presenta problemas de completitud, duplicidad, fechas invalidas o integridad referencial, y en que fuente o periodo se concentran?
2. ¿Que embalses presentan menor almacenamiento relativo respecto a su capacidad util y como cambia esta condicion en el tiempo?
3. ¿Que rios presentan aportes por debajo de su media historica y durante cuanto tiempo se mantiene esta condicion?
4. ¿Como cambia la generacion hidroelectrica entre periodos con diferentes condiciones hidricas?
5. ¿Como se comporta el precio de bolsa durante periodos con diferentes condiciones hidricas?

## 6. Inventario de fuentes

### Fuente principal actual: API XM

Metricas diarias: `PorcVoluUtilDiar`, `VoluUtilDiarMasa`, `CapaUtilDiarMasa`, `AporCaudal`, `AporCaudalMediHist`, `PorcApor`.

Metricas horarias: `Gene`, `PrecBolsNaci`.

Catalogos: `ListadoEmbalses`, `ListadoRios`, `ListadoRecursos`.

**Pendiente:** incorporar una segunda fuente de tipo distinto para cumplir completamente la condicion minima de la rubrica. No se contabiliza un catalogo de la misma API como una fuente externa independiente.

## 7. Modelo conceptual

Entidades principales:

- **Embalse:** posee observaciones diarias de almacenamiento/capacidad.
- **Rio:** posee observaciones diarias de aportes y referencias historicas.
- **Recurso de generacion:** posee observaciones horarias de generacion y atributos de catalogo.
- **Sistema:** posee observaciones horarias de precio de bolsa.
- **Tiempo:** fecha, hora, ano y periodos que permiten homologar granularidades para consultas.

Relaciones principales:

- `Gene.entidad` se relaciona con `ListadoRecursos.Code`.
- Las metricas hidricas se relacionan analiticamente por fecha/periodo, evitando interpretar una asociacion temporal como causalidad.

## 8. Alternativas arquitectonicas

### Alternativa A - Procesamiento local con archivos y pandas

Ventajas:

- menor complejidad inicial;
- facil exploracion en un solo equipo;
- suficiente para muestras pequenas.

Limitaciones:

- almacenamiento no distribuido;
- menor evidencia de escalamiento y tolerancia a fallos;
- dependencia de memoria/CPU de una sola maquina;
- menor alineacion con el objetivo del curso de computacion distribuida.

### Alternativa B - Docker Compose + HDFS + Spark + Jupyter

Ventajas:

- infraestructura reproducible;
- almacenamiento distribuido;
- procesamiento paralelo con Spark;
- trazabilidad por zonas;
- posibilidad de observar particiones, shuffle, workers y stages;
- separacion clara entre almacenamiento, procesamiento y consumo analitico.

Limitaciones:

- mayor complejidad operativa;
- todos los contenedores pueden seguir compartiendo un mismo host fisico en el entorno academico;
- el volumen actual puede caber en una estacion de trabajo, por lo que la necesidad de distribucion se justifica tambien por el objetivo de escalamiento y aprendizaje, no solo por imposibilidad absoluta de procesamiento local.

### Seleccion

Se selecciono la alternativa B debido a los objetivos del curso, la reproducibilidad requerida y la necesidad de demostrar almacenamiento/procesamiento distribuido y escalamiento.

## 9. Arquitectura elegida

```text
API XM
  -> Python requests (batch)
  -> HDFS RAW
  -> Spark DataFrames
  -> calidad + normalizacion
  -> HDFS CURATED / Parquet
  -> Jupyter + Spark SQL
  -> HDFS ANALYTICS
  -> consultas, indicadores y visualizaciones
```

Zonas:

```text
/proyecto_xm/raw
/proyecto_xm/curated
/proyecto_xm/analytics
/proyecto_xm/quality/rejected
/proyecto_xm/quality/corrected
```

## 10. Plan de pruebas

El plan incluye validacion de infraestructura, DataNodes, volumen, calidad, integridad referencial, JOIN sin multiplicacion, resultado conocido, Spark shuffle, tiempos, escalamiento, idempotencia y recuperacion. Ver `docs/plan_pruebas.md`.

## 11. Riesgos

- cambios en la estructura/disponibilidad de la API;
- unidades o definiciones de metrica mal interpretadas si no se verifican documentalmente;
- catalogo vigente utilizado con historia desde 2020;
- archivos pequenos/particiones excesivas;
- sesgo al comparar granularidades horarias y diarias;
- interpretar asociaciones como causalidad;
- perdida de HDFS si se eliminan volumenes Docker con `docker compose down -v`.

## 12. Estado de la entrega

La arquitectura se encuentra implementada y la ingesta/curacion principal ya fue ejecutada. Permanecen pendientes algunos requisitos finales, en especial la segunda fuente de tipo distinto, la materializacion ANALYTICS, las consultas finales y las pruebas de escala/recuperacion.
