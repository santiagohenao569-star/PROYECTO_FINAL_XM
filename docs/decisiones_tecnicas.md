# Decisiones técnicas - Proyecto Final XM

Proyecto: Infraestructura Big Data para análisis del recurso hídrico con datos de XM

Este documento registra las principales decisiones de arquitectura e implementación tomadas durante el desarrollo del proyecto. Cada decisión puede ser revisada si las pruebas, el volumen real de datos o las restricciones del entorno muestran la necesidad de modificarla.

---

## DT-001 - Uso del laboratorio integrado como línea base

### Situación

Durante los laboratorios previos se implementó y validó una plataforma compuesta por HDFS, Apache Spark, JupyterLab y Kafka utilizando Docker Compose.

### Alternativas consideradas

**Alternativa A:** construir nuevamente toda la infraestructura desde cero.

**Alternativa B:** utilizar la infraestructura validada en el laboratorio como línea base y adaptarla al caso de la API XM.

### Decisión

Se seleccionó la alternativa B.

### Justificación

La infraestructura del laboratorio ya permitió comprobar el funcionamiento de HDFS distribuido, Spark con múltiples Workers, JupyterLab y Kafka. Reutilizarla como referencia permite concentrar el proyecto final en la adaptación de la plataforma al problema del recurso hídrico, manteniendo la reproducibilidad mediante Docker Compose.

La infraestructura no será utilizada sin modificaciones. Se revisarán nombres de servicios, configuración Hadoop, rutas, volúmenes, dependencias y componentes necesarios para eliminar elementos específicos del laboratorio anterior.

### Limitación

El proyecto se ejecutará en un computador mediante contenedores Docker. Por lo tanto, aunque existan diferentes nodos lógicos, estos comparten el mismo host físico y no representan un clúster de producción completamente distribuido.

### Estado

En construcción.

---

## DT-002 - Número de DataNodes de HDFS

### Alternativas consideradas

**Alternativa A:** utilizar 2 DataNodes.

**Alternativa B:** utilizar 3 DataNodes.

### Decisión

Se utilizarán inicialmente **2 DataNodes**.

### Justificación

Dos DataNodes son suficientes para demostrar los principales conceptos requeridos en el proyecto:

- almacenamiento distribuido;
- división de archivos en bloques;
- replicación;
- ubicación de bloques en diferentes nodos;
- prueba controlada de disponibilidad ante el fallo de un DataNode.

Utilizar un tercer DataNode incrementaría el consumo de recursos del equipo sin que actualmente exista un requisito de la rúbrica o del caso de negocio que lo justifique.

### Evidencia futura

La decisión será validada mediante:

- interfaz web del NameNode;
- DataNodes activos;
- ejecución de `hdfs fsck`;
- ubicación de bloques;
- prueba de fallo o recuperación.

### Estado

Decidido provisionalmente. Pendiente de validación durante la implementación.

---

## DT-003 - Factor de replicación de HDFS

### Situación observada en el laboratorio

Durante el laboratorio integrado se utilizaron 2 DataNodes. Sin embargo, algunos archivos escritos desde Spark fueron creados solicitando un factor de replicación diferente al configurado en los servicios HDFS.

Esto produjo bloques subreplicados, aunque los datos permanecieron disponibles y no se presentaron bloques corruptos o perdidos.

### Decisión preliminar

Se buscará que el factor de replicación utilizado por el proyecto sea coherente con el número de DataNodes disponibles.

Con 2 DataNodes se espera utilizar:

`dfs.replication = 2`

### Justificación

Un factor de replicación de 2 permitiría almacenar una copia de cada bloque en cada DataNode, proporcionando redundancia básica sin solicitar más réplicas de las que la infraestructura puede mantener físicamente.

### Condición antes de implementar

La decisión se confirmará después de revisar el archivo `hadoop.env` y comprobar qué configuración Hadoop recibe el cliente Spark.

### Mejora respecto al laboratorio

El cliente Spark deberá utilizar la misma configuración Hadoop que los servicios HDFS para evitar diferencias entre la configuración del clúster y la utilizada durante la escritura de archivos.

### Estado

Pendiente de validación de configuración.

---

## DT-004 - Tamaño de bloque HDFS

### Alternativas

El laboratorio anterior utiliza una configuración específica de tamaño de bloque. También existen tamaños de bloque mayores utilizados habitualmente en sistemas HDFS.

### Decisión

**No se define todavía un tamaño definitivo de bloque.**

### Justificación

El tamaño de bloque se seleccionará después de conocer:

- volumen real descargado desde la API XM;
- tamaño promedio de los archivos;
- cantidad de registros;
- estrategia de particionamiento;
- comportamiento de lectura mediante Spark.

La elección no se realizará únicamente por utilizar un valor predeterminado habitual.

### Criterio de decisión

Se buscará evitar una cantidad excesiva de archivos pequeños y, al mismo tiempo, mantener suficiente paralelismo para las pruebas del proyecto.

### Estado

Pendiente.

---

## DT-005 - Configuración del clúster Spark

### Alternativas consideradas

**Alternativa A:** procesamiento Spark local con un único proceso.

**Alternativa B:** 1 Spark Master y 2 Spark Workers.

### Decisión

Se utilizará:

- 1 Spark Master;
- 2 Spark Workers.

Como configuración inicial, cada Worker conservará los recursos utilizados en el laboratorio:

- 2 cores;
- 1 GB de memoria.

### Justificación

Esta estructura permite demostrar procesamiento distribuido y observar en Spark UI:

- Workers;
- Executors;
- Jobs;
- Stages;
- Tasks;
- operaciones de shuffle.

La configuración de CPU y memoria podrá ajustarse posteriormente si las pruebas de volumen muestran que es necesario.

### Evidencia futura

Se capturarán:

- Spark Master con los Workers activos;
- Executors utilizados;
- Jobs y Stages de procesamiento;
- Tasks ejecutadas en ambos Workers.

### Estado

Decidido provisionalmente.

---

## DT-006 - Integración entre Spark y HDFS

### Decisión

Apache Spark leerá los datos directamente desde HDFS y escribirá nuevamente en HDFS los resultados de las transformaciones.

El flujo previsto es:

API XM  
→ ingesta Python  
→ HDFS Raw  
→ Spark DataFrames  
→ reglas de calidad y transformación  
→ HDFS Curated en Parquet  
→ Spark SQL  
→ HDFS Analytics.

### Justificación

Esta arquitectura separa la ingesta, el almacenamiento distribuido y el procesamiento.

Permite demostrar que los datos utilizados por Spark no dependen de archivos ubicados únicamente en el sistema local del usuario.

### Requisito técnico

Spark deberá recibir explícitamente la configuración necesaria para localizar el NameNode y utilizar parámetros Hadoop coherentes con el clúster.

### Estado

En construcción.

---

## DT-007 - Uso de Parquet en las zonas Curated y Analytics

### Alternativas consideradas

**CSV/JSON:** formatos simples y adecuados para conservar datos originales.

**Parquet:** formato columnar orientado a procesamiento analítico.

### Decisión

La zona Raw conservará la información en un formato cercano a la respuesta original obtenida de XM.

Las zonas Curated y Analytics utilizarán principalmente **Parquet**.

### Justificación

Parquet permite:

- lectura por columnas;
- compresión;
- conservación de tipos de datos;
- integración eficiente con Spark;
- reducción de lectura innecesaria en consultas analíticas.

### Estado

Decidido.

---

## DT-008 - Uso de Kafka

### Alternativas consideradas

**Alternativa A:** utilizar Kafka desde el inicio.

**Alternativa B:** realizar primero una ingesta batch desde la API XM y agregar Kafka únicamente si existe una necesidad de procesamiento continuo.

### Decisión

Kafka se mantendrá como **componente opcional** durante la primera etapa.

### Justificación

La rúbrica exige ingesta batch. El uso de streaming debe estar justificado por el problema y no únicamente por disponer de la tecnología.

La información histórica de XM puede consumirse mediante solicitudes periódicas a la API y almacenarse en HDFS.

Kafka será incorporado posteriormente solo si se define una necesidad como:

- simulación de llegada continua de nuevas observaciones;
- monitoreo periódico;
- demostración de actualización incremental;
- procesamiento Structured Streaming.

### Estado

Pendiente de decisión final.

---

## DT-009 - JupyterLab como entorno de exploración

### Decisión

JupyterLab se ejecutará dentro del contenedor `spark-client`.

### Justificación

Esto permite que los notebooks utilicen el mismo entorno de red que HDFS y Spark y facilita la reproducción de:

- consultas a la API;
- validaciones;
- procesamiento Spark;
- Spark SQL;
- análisis de resultados.

Los notebooks se conservarán dentro del repositorio y estarán disponibles mediante un volumen Docker compartido.

### Estado

Decidido.

---

## DT-010 - Estrategia de zonas de datos

### Decisión

HDFS se organizará inicialmente en las siguientes zonas:

`raw`  
Información recibida desde la fuente con transformaciones mínimas.

`curated`  
Datos tipificados, validados, deduplicados y preparados para análisis.

`analytics`  
Resultados agregados o indicadores utilizados por las consultas de negocio.

`quality/rejected`  
Registros que no cumplen las reglas de calidad definidas.

### Estructura preliminar

`/proyecto_xm/raw/`

`/proyecto_xm/curated/`

`/proyecto_xm/analytics/`

`/proyecto_xm/quality/rejected/`

### Justificación

La separación permite conservar trazabilidad entre el dato recibido, el dato validado y los resultados analíticos.

### Estado

Decidido provisionalmente.

---

## DT-011 - Control de versiones y colaboración

### Decisión

Se utilizarán Git y GitHub.

La rama `main` mantendrá la versión integrada del proyecto y los integrantes trabajarán posteriormente mediante ramas específicas antes de incorporar los cambios.

### Justificación

Esta estrategia permite:

- mantener historial;
- identificar autores de cambios;
- recuperar versiones anteriores;
- integrar el trabajo de varios integrantes;
- conservar versiones asociadas a las entregas del proyecto.

### Estado

Implementado parcialmente. Pendiente agregar los demás integrantes al repositorio.
