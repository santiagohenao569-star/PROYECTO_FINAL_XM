# Informe final consolidado - Borrador vivo

> Este archivo se completa progresivamente. No convertir los campos PENDIENTE en conclusiones hasta ejecutar y verificar las consultas/pruebas correspondientes.

## Resumen ejecutivo

Se construyo un prototipo distribuido para ingerir, almacenar, validar y consultar informacion publica de XM relacionada con recurso hidrico, generacion y precio de bolsa. La solucion utiliza Docker Compose, HDFS y Apache Spark, y separa los datos en zonas RAW, CURATED, QUALITY y ANALYTICS.

Hasta el estado actual se han procesado 12.296.013 eventos horarios validos de generacion y se han aplicado reglas de calidad sobre ocho metricas historicas. El enriquecimiento de generacion con el catalogo de recursos mantuvo exactamente el mismo numero de filas antes y despues del JOIN, evitando multiplicacion de observaciones. Se identificaron 160 recursos hidraulicos presentes en `Gene` y 25 codigos historicos sin correspondencia en el catalogo vigente.

Las respuestas finales a las preguntas de negocio, la prueba de escalamiento, el analisis de shuffle/desempeno y la recuperacion ante fallos permanecen PENDIENTES en este borrador.

## 1. Contexto y preguntas

[Tomar como base `docs/01_informe_entrega1.md`]

Preguntas finales:

1. Calidad y trazabilidad.
2. Embalses con menor almacenamiento relativo y evolucion.
3. Rios por debajo de media historica y duracion.
4. Generacion hidroelectrica segun condicion hidrica.
5. Precio de bolsa segun condicion hidrica.

## 2. Datos

Periodo: 2020-01-01 a 2026-11-19.

Fuente principal actual: API XM.

**PENDIENTE:** incorporar/justificar segunda fuente de tipo distinto.

Describir aqui:

- metricas y granularidad;
- volumen;
- unidades verificadas;
- calidad;
- catalogos;
- sesgos/limitaciones.

## 3. Arquitectura

```text
API XM -> Python batch -> HDFS RAW -> Spark -> HDFS CURATED -> Jupyter/Spark SQL -> HDFS ANALYTICS
```

Incluir diagrama final y responsabilidades de Docker, HDFS, Spark, Jupyter, Parquet, Git y Kafka opcional.

## 4. Implementacion

### 4.1 Infraestructura

Docker Compose con NameNode, dos DataNodes, Spark Master, dos Workers, Spark Client/Jupyter y Kafka.

### 4.2 Ingesta

Describir ventanas API, reintentos, RAW y estrategia de reejecucion.

### 4.3 Calidad y normalizacion

Incluir R1-R4 y tabla por metrica.

### 4.4 Catalogos y enriquecimiento

Incluir duplicados del catalogo, curacion, LEFT JOIN y validacion de filas.

## 5. Validacion

### 5.1 Volumen

Gene: 12.296.013 eventos validos.

### 5.2 Calidad

Gene: 3.606.387 posiciones horarias sin valor clasificadas actualmente por completitud.

### 5.3 Integridad referencial

25 de 672 codigos distintos de Gene no aparecen en el catalogo vigente; corresponden a 112.246 eventos validos. Se mantiene el registro y se marca ausencia de correspondencia.

### 5.4 JOIN

12.296.013 filas antes y despues.

### 5.5 Resultado conocido

**PENDIENTE.**

### 5.6 Desempeno, particiones y shuffle

**PENDIENTE.**

### 5.7 Escalamiento

**PENDIENTE:** dos volumenes y estrategia 10x.

### 5.8 Reinicio/idempotencia/recuperacion

**PENDIENTE de prueba formal.**

## 6. Resultados de consultas

### Consulta 1 - Calidad y trazabilidad

**PENDIENTE de consulta final.**

Estructura a completar:

- pregunta;
- poblacion/periodo;
- consulta Spark SQL;
- resultado;
- hallazgo;
- interpretacion;
- limitacion;
- decision que apoya.

### Consulta 2 - Embalses

**PENDIENTE.**

### Consulta 3 - Aportes hidricos

**PENDIENTE.**

### Consulta 4 - Hidrologia y generacion

**PENDIENTE.**

### Consulta 5 - Hidrologia y precio

**PENDIENTE.**

## 7. Limitaciones y riesgos

- Catalogos vigentes frente a datos historicos.
- Segunda fuente aun pendiente.
- Unidades pendientes de verificacion documental en este borrador.
- Cluster distribuido ejecutado en un mismo host fisico.
- Posibles diferencias de granularidad entre series diarias y horarias.
- Datos faltantes no explicados causalmente.
- Asociaciones entre hidrologia, generacion y precio no implican causalidad.

## 8. Costos aproximados

**PENDIENTE:** documentar recursos locales utilizados y elaborar una estimacion simple solo si se cuenta con supuestos verificables. No inventar costos de nube.

## 9. Mejoras futuras

- dimension historica de recursos;
- segunda fuente externa complementaria;
- automatizacion/orquestacion;
- evaluacion de particionamiento segun consultas;
- monitoreo de calidad por periodo/fuente;
- streaming solo ante una necesidad de latencia real.

## 10. Conclusiones

**PENDIENTE:** redactar despues de ejecutar las cinco consultas y pruebas finales. Las conclusiones deben diferenciar datos, hallazgos, limitaciones y recomendaciones.

## 11. Uso de asistencia generativa

Se utilizo asistencia generativa para apoyar explicaciones conceptuales, estructuracion de documentacion, revision de codigo y diseno de procedimientos de prueba. El equipo ejecuto y verifico los comandos/resultados en su propio entorno y conserva la responsabilidad de comprender y defender la solucion.
