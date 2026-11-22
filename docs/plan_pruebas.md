# Plan y matriz de pruebas

## Criterio general

Cada prueba debe registrar: fecha, responsable, comando o notebook, dataset/periodo, resultado esperado, resultado obtenido, estado y evidencia asociada.

| ID | Prueba | Resultado esperado | Estado actual | Evidencia sugerida |
|---|---|---|---|---|
| T01 | Levantar infraestructura | Servicios principales `Up` | CUMPLIDO | `E01_docker_compose_ps.png` |
| T02 | Verificar HDFS | 2 DataNodes vivos | CUMPLIDO | `E02_hdfs_datanodes.png` |
| T03 | Verificar zonas HDFS | RAW/CURATED/QUALITY visibles | CUMPLIDO | `E03_hdfs_zonas.png` |
| T04 | Conteo de `Gene` | 12.296.013 eventos horarios con valor | CUMPLIDO | `E05_conteo_gene.png` |
| T05 | Calidad `Gene` | Rechazados clasificados y separados | CUMPLIDO | `E06_calidad_gene.png` |
| T06 | Curacion catalogo | 2.519 codigos unicos desde 2.522 registros | CUMPLIDO | `E09_catalogo_curado.png` |
| T07 | Integridad referencial | Identificar codigos Gene sin catalogo | CUMPLIDO: 25 codigos | `E08_integridad_catalogo.png` |
| T08 | JOIN sin multiplicacion | Filas antes = filas despues | CUMPLIDO: 12.296.013 = 12.296.013 | `E10_join_gene_recursos.png` |
| T09 | Clasificacion hidraulica | Conteo reproducible de recursos/eventos | CUMPLIDO: 160 recursos, 7.277.050 eventos | `E10_join_gene_recursos.png` |
| T10 | Resultado conocido | Consulta sobre subconjunto pequeno coincide con calculo manual | PENDIENTE | `E14_resultado_controlado.png` |
| T11 | Salida ANALYTICS | Parquet/resultados finales visibles en HDFS | PENDIENTE | `E20_analytics_hdfs.png` |
| T12 | Cinco consultas | Las 5 preguntas producen resultado verificable | PENDIENTE | E15-E19 |
| T13 | Spark plan/shuffle | Evidenciar operacion con shuffle y stages | PENDIENTE | `E21_spark_ui_shuffle.png` |
| T14 | Tiempo de ejecucion | Registrar tiempo y volumen de al menos un job | PENDIENTE | `E22_tiempos.png` |
| T15 | Dos volumenes | Comparar conjunto controlado vs ampliado | PENDIENTE | `E23_escala_dos_volumenes.png` |
| T16 | Estrategia 10x | Generar/procesar volumen 10x o justificar estrategia reproducible | PENDIENTE | `E24_escala_10x.png` |
| T17 | Reejecucion idempotente | Reejecutar sin duplicar archivos/resultados | PENDIENTE FORMAL | `E25_idempotencia.png` |
| T18 | Reinicio/recuperacion | Reiniciar componente definido y recuperar servicio/datos | PENDIENTE | `E26_recuperacion.png` |
| T19 | README en otro equipo | Otro integrante reproduce el flujo documentado | PENDIENTE | Registro de prueba/evidencia |

## Pruebas ya sustentadas

### T04 - Volumen Gene

- Posiciones normalizadas: 15.902.400.
- Eventos con valor valido: 12.296.013.
- Posiciones sin valor: 3.606.387.

### T06 - Catalogo

- Registros RAW: 2.522.
- Codigos unicos CURATED: 2.519.
- Codigos con duplicidad: 3.
- Codigos con conflicto de nombre: 1.
- Conflicto en otros atributos observado: 0.

### T07 - Integridad referencial

- Recursos distintos en `Gene`: 672.
- Encontrados en catalogo actual: 647.
- No encontrados: 25.

### T08/T09 - Enriquecimiento

- Filas antes: 12.296.013.
- Filas despues: 12.296.013.
- Eventos sin catalogo: 112.246.
- Recursos sin catalogo: 25.
- Eventos hidraulicos: 7.277.050.
- Recursos hidraulicos: 160.

## Registro de ejecucion

Agregar una fila por prueba ejecutada:

| Fecha | ID | Responsable | Resultado | Estado | Evidencia |
|---|---|---|---|---|---|
| Pendiente |  |  |  |  |  |
