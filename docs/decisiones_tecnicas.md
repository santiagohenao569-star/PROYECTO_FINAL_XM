# Registro de decisiones tecnicas

Este archivo registra decisiones de arquitectura para que el equipo pueda explicar no solo que tecnologia uso, sino por que la uso.

## DT-001 - Ingesta batch como flujo principal

**Decision:** consultar la API de XM mediante Python/requests y almacenar los resultados por ventanas temporales.

**Razon:** el caso se basa principalmente en informacion historica y no requiere latencia de segundos para responder las preguntas priorizadas. El batch reduce complejidad y cumple el flujo obligatorio del proyecto.

**Implicacion:** Kafka queda disponible como componente experimental, pero no se presenta como necesario para el pipeline principal mientras no exista una pregunta que justifique streaming.

## DT-002 - Infraestructura reproducible con Docker Compose

**Decision:** ejecutar Hadoop/HDFS, Spark, Jupyter y Kafka en contenedores coordinados con Docker Compose.

**Razon:** facilita reproducir la misma infraestructura en los equipos del grupo sin instalaciones manuales independientes de Hadoop y Spark.

## DT-003 - Dos DataNodes y replicacion HDFS igual a 2

**Decision:** desplegar dos DataNodes y configurar `dfs.replication=2`.

**Razon:** permite observar almacenamiento distribuido y mantener dos replicas de los bloques dentro del entorno academico.

**Limitacion:** no representa alta disponibilidad de produccion; es un laboratorio distribuido en un mismo host fisico.

## DT-004 - Tamano de bloque HDFS de 16 MiB

**Decision:** utilizar 16 MiB como configuracion provisional del bloque HDFS y del bloque Parquet.

**Razon:** se adopto para el entorno academico y el volumen actual, permitiendo observar bloques/particiones sin requerir archivos extremadamente grandes.

**Pendiente:** comparar su efecto con el volumen y las consultas finales antes de presentarlo como configuracion optima.

## DT-005 - Spark Master con dos Workers

**Decision:** desplegar un Spark Master y dos Workers.

**Razon:** permite ejecutar DataFrames de forma distribuida y observar jobs, stages, tasks y movimiento de datos.

## DT-006 - HDFS como almacenamiento principal

**Decision:** configurar Spark para leer y escribir directamente en `hdfs://namenode:9000`.

**Razon:** separar procesamiento de almacenamiento y cumplir el requisito de almacenamiento distribuido.

## DT-007 - Parquet en CURATED

**Decision:** guardar los datos normalizados/curados en Parquet.

**Razon:** formato columnar apropiado para consultas analiticas con Spark, con menor costo de lectura de columnas que JSON.

## DT-008 - Kafka opcional, no forzado

**Decision:** no convertir Kafka en requisito del flujo principal solo por estar disponible.

**Razon:** el proyecto debe justificar cada tecnologia. El caso actual se responde con batch; streaming se incorporaria solo si una necesidad de latencia lo sustenta.

## DT-009 - Jupyter como cliente interactivo

**Decision:** utilizar JupyterLab conectado al cluster Spark para exploracion, Spark SQL, visualizacion y documentacion de consultas.

**Razon:** facilita ejecutar paso a paso y explicar el analisis durante la defensa.

## DT-010 - Separacion por zonas

**Decision:** utilizar rutas HDFS separadas para RAW, CURATED, ANALYTICS y QUALITY.

**Razon:** conservar el dato original, evitar correcciones silenciosas y permitir trazabilidad del pipeline.

## DT-011 - Git y GitHub para codigo, no para datos masivos

**Decision:** versionar configuracion, scripts, notebooks, SQL, documentacion y evidencia seleccionada; mantener los datos masivos en HDFS.

**Razon:** Git no es un sistema de almacenamiento de datasets masivos y la reproducibilidad se logra mediante codigo de ingesta/procesamiento.

## DT-012 - Curar el catalogo antes de cruzarlo con Gene

**Decision:** reducir `ListadoRecursos` a una fila por codigo antes del JOIN.

**Evidencia:** 2.522 registros RAW, 2.519 codigos unicos, 3 codigos duplicados y un conflicto de nombre.

**Razon:** un JOIN directo contra claves duplicadas puede multiplicar artificialmente los eventos de generacion.

## DT-013 - LEFT JOIN para enriquecer Gene

**Decision:** cruzar `Gene` con el catalogo mediante LEFT JOIN.

**Razon:** se deben conservar los eventos historicos aunque el codigo no aparezca en el catalogo vigente.

**Validacion:** 12.296.013 filas antes y 12.296.013 despues del JOIN.

## DT-014 - No declarar como error los codigos historicos sin catalogo actual

**Decision:** marcar `correspondencia_catalogo=false` sin eliminar los registros.

**Evidencia:** 25 codigos y 112.246 eventos no tienen correspondencia en el catalogo actual.

**Razon:** el catalogo descargado es una fotografia vigente y `Gene` cubre historia desde 2020. No existe evidencia suficiente para afirmar que esos eventos sean erroneos.

## DT-015 - Clasificacion de recurso hidraulico

**Decision:** clasificar como hidraulico cuando el catalogo indique simultaneamente `Type = HIDRAULICA` y `EnerSource = AGUA`.

**Razon:** utilizar atributos explicitos del catalogo y no inferir la tecnologia a partir del nombre del recurso.

**Resultado actual:** 160 recursos y 7.277.050 eventos validos clasificados como hidraulicos.
