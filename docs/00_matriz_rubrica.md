# Matriz de control de la rúbrica - Proyecto Final XM

Proyecto: Infraestructura Big Data para análisis del recurso hídrico con datos de XM

Estados:
- ⏳ Pendiente
- 🟡 En construcción
- ✅ Completado

| ID | Entrega / criterio | Qué se implementará | Evidencia esperada | Dónde se documenta | Estado |
|---|---|---|---|---|---|
| E1-01 | Contexto, usuario y proceso | Definir el contexto del recurso hídrico y su relación con la operación del SIN | Descripción del problema y usuario de la información | `docs/01_entrega1_diagnostico_arquitectura.md` | 🟡 |
| E1-02 | Problema, alcance, restricciones y criterio de éxito | Delimitar qué analizará la plataforma y qué queda fuera del alcance | Sección de alcance, restricciones y criterios de éxito | `docs/01_entrega1_diagnostico_arquitectura.md` | 🟡 |
| E1-03 | Preguntas de negocio priorizadas | Definir entre 3 y 5 preguntas relacionadas con recurso hídrico, generación y precios | Lista de preguntas con usuario, métrica y decisión | `docs/01_entrega1_diagnostico_arquitectura.md` | 🟡 |
| E1-04 | Inventario de fuentes | Consultar API XM y documentar métricas, frecuencia, granularidad, acceso y volumen | Catálogo de métricas XM y diccionario inicial | `docs/01_entrega1_diagnostico_arquitectura.md` y `data/sample/` | ⏳ |
| E1-05 | Modelo conceptual | Relacionar fecha, río, embalse, recurso de generación, precio y demás entidades seleccionadas | Diagrama o esquema conceptual | `docs/01_entrega1_diagnostico_arquitectura.md` | ⏳ |
| E1-06 | Comparación de arquitecturas | Comparar una alternativa local/simple frente a HDFS + Spark distribuido | Tabla comparativa con ventajas, limitaciones y decisión | `docs/decisiones_tecnicas.md` y Entrega 1 | 🟡 |
| E1-07 | Arquitectura seleccionada | API XM → ingesta → HDFS raw → Spark → curated → analytics → consultas | Diagrama de arquitectura con responsabilidades | `docs/01_entrega1_diagnostico_arquitectura.md` | 🟡 |
| E1-08 | Plan de pruebas, riesgos y cronograma | Definir pruebas de exactitud, calidad, escalamiento, fallo e idempotencia | Matriz de pruebas y riesgos | `docs/01_entrega1_diagnostico_arquitectura.md` | ⏳ |
| E1-09 | Repositorio y README | Mantener estructura versionada y reproducible en Git/GitHub | Repositorio, historial de commits y README | `README.md` | 🟡 |
| E2-01 | Infraestructura reproducible | Adaptar Docker Compose para HDFS, Spark y Jupyter | Servicios activos y archivo `compose.yaml` versionado | `README.md`, `docs/02_entrega2_prototipo.md`, `evidence/01_arquitectura/` | 🟡 |
| E2-02 | Almacenamiento distribuido | Configurar HDFS con NameNode y múltiples DataNodes | NameNode UI, DataNodes activos y `fsck` | `docs/02_entrega2_prototipo.md`, `evidence/03_hdfs/` | ⏳ |
| E2-03 | Ingesta batch | Consumir datos reales desde la API XM y almacenarlos en zona raw de HDFS | Código de ingesta, código HTTP, conteos y archivos en HDFS | `ingestion/`, `notebooks/01_ingesta_xm.ipynb`, `evidence/02_ingesta/` | ⏳ |
| E2-04 | Volumen de datos | Cargar al menos 250.000 registros o equivalente justificado | Conteo de registros mediante Spark | `docs/02_entrega2_prototipo.md` | ⏳ |
| E2-05 | Zonas raw y curated | Conservar datos originales en raw y publicar datos validados en curated | Rutas HDFS, formatos y particiones | `docs/02_entrega2_prototipo.md`, `evidence/03_hdfs/` | ⏳ |
| E2-06 | Calidad de datos | Aplicar mínimo cuatro reglas: completitud, duplicados, fechas, tipos e integridad referencial | Conteo de aceptados/rechazados y motivos | `spark/`, `docs/02_entrega2_prototipo.md`, `evidence/05_calidad/` | ⏳ |
| E2-07 | Procesamiento Spark | Procesar los datos con DataFrames/Spark SQL en el clúster | Spark Jobs, Stages, Tasks y Executors | `spark/`, `notebooks/`, `evidence/04_spark/` | ⏳ |
| E2-08 | Particiones y shuffle | Ejecutar al menos una operación distribuida que genere shuffle, como `groupBy` o `join` | Spark UI y plan de ejecución | `docs/02_entrega2_prototipo.md`, `evidence/04_spark/` | ⏳ |
| E2-09 | Zona analytics | Materializar resultados analíticos en Parquet | Archivos Parquet en HDFS analytics | `spark/`, `evidence/03_hdfs/` | ⏳ |
| E2-10 | Consultas parciales | Resolver dos consultas de negocio y una técnica durante el prototipo | Resultados de Spark SQL/DataFrames | `sql/`, `notebooks/`, `docs/02_entrega2_prototipo.md` | ⏳ |
| E3-01 | Pipeline extremo a extremo | Ejecutar desde API XM hasta consulta final | Demostración completa y evidencias secuenciales | `docs/03_entrega3_integracion_validacion.md` | ⏳ |
| E3-02 | Cinco consultas mínimas | Resolver las cinco consultas definidas y justificar las respuestas | Tablas, indicadores y visualizaciones | `sql/`, `notebooks/`, `docs/03_entrega3_integracion_validacion.md` | ⏳ |
| E3-03 | Consulta de calidad o trazabilidad | Medir faltantes, duplicados, fechas inválidas e integridad por fuente/período | Consulta técnica con cantidad y porcentaje | `sql/`, `evidence/05_calidad/` | ⏳ |
| E3-04 | Consulta temporal | Analizar evolución del recurso hídrico por fechas o ventanas | Serie temporal o agregación por período | `notebooks/`, `docs/03_entrega3_integracion_validacion.md` | ⏳ |
| E3-05 | Consulta combinando fuentes | Relacionar hidrología con generación, precio u otra fuente XM | Join entre fuentes y resultado validado | `spark/`, `sql/`, `evidence/06_consultas/` | ⏳ |
| E3-06 | Indicador para decisión | Construir al menos un indicador relacionado con una decisión | Fórmula, interpretación y resultado | `docs/03_entrega3_integracion_validacion.md` | ⏳ |
| E3-07 | Dataset controlado vs ampliado | Comparar resultados de una muestra conocida con el conjunto completo | Conteos y resultados comparables | `tests/`, `docs/03_entrega3_integracion_validacion.md` | ⏳ |
| E3-08 | Escalamiento | Comparar al menos dos volúmenes y preparar estrategia 10x | Tiempos, volumen, número de particiones y recursos | `tests/`, `evidence/07_validacion/` | ⏳ |
| E3-09 | Análisis de particiones y almacenamiento | Evaluar particiones, shuffle, tiempos y tamaño de archivos | Métricas Spark/HDFS | `docs/03_entrega3_integracion_validacion.md` | ⏳ |
| E3-10 | Recuperación / idempotencia | Probar reinicio, reejecución sin duplicados o fallo controlado | Evidencia antes/después y resultado de recuperación | `tests/`, `evidence/07_validacion/` | ⏳ |
| E3-11 | Limitaciones, riesgos y mejoras | Declarar limitaciones técnicas y del análisis | Sección final con riesgos y evolución propuesta | `docs/03_entrega3_integracion_validacion.md` | ⏳ |
| E3-12 | README final | Documentar instalación, ejecución, consultas, puertos y limpieza | README reproducible | `README.md` | ⏳ |
| E3-13 | Defensa | Preparar demostración de arquitectura, ingesta, HDFS, Spark y consultas | Presentación y secuencia de demostración | `docs/` y `evidence/` | ⏳ |

## Consultas previstas

1. Calidad y trazabilidad:
   ¿Qué porcentaje de registros presenta problemas de completitud, duplicidad, fechas inválidas o integridad referencial, y en qué fuente o período se concentran?

2. Embalses:
   ¿Qué embalses presentan menor almacenamiento relativo respecto a su capacidad útil y cómo cambia esta condición en el tiempo?

3. Aportes hídricos:
   ¿Qué ríos presentan aportes por debajo de su media histórica y durante cuánto tiempo se mantiene esta condición?

4. Hidrología y generación:
   ¿Cómo cambia la generación hidroeléctrica entre períodos con diferentes condiciones hídricas?

5. Hidrología y precio:
   ¿Cómo se comporta el precio de bolsa durante períodos con diferentes condiciones hídricas?

## Arquitectura preliminar

API XM
→ Ingesta Python
→ HDFS Raw
→ Apache Spark
→ Reglas de calidad
→ HDFS Curated / Parquet
→ Spark SQL
→ HDFS Analytics
→ Consultas e indicadores

Configuración preliminar de infraestructura:

- 1 NameNode.
- 2 DataNodes.
- Factor de replicación HDFS por definir después de validar `hadoop.env`; inicialmente se espera coherencia con el número de DataNodes.
- 1 Spark Master.
- 2 Spark Workers.
- JupyterLab integrado con Spark.
- Kafka se mantiene como componente opcional y solo se utilizará si se justifica una necesidad de streaming.

## Regla de documentación

Cada cambio técnico relevante deberá registrar:

1. Qué se modificó.
2. Por qué se modificó.
3. Qué requisito de la rúbrica cubre.
4. Qué evidencia valida el resultado.
5. Qué limitación o riesgo permanece.
