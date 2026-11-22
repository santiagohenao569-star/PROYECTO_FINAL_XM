# Matriz de cumplimiento de la rubrica

Estado de control del proyecto. La matriz debe actualizarse a medida que se generen nuevas evidencias.

| Requisito | Estado | Evidencia actual | Pendiente |
|---|---|---|---|
| Infraestructura reproducible con Docker Compose | CUMPLIDO | `compose.yaml`, servicios HDFS/Spark/Kafka/Jupyter | Captura final `docker compose ps` |
| Dos tipos de fuente | PENDIENTE | API XM historica y catalogos XM pertenecen al mismo ecosistema | Incorporar y justificar una segunda fuente de tipo distinto |
| Una fuente con eventos/telemetria | CUMPLIDO | `Gene` y `PrecBolsNaci` horarios | Documentar formalmente |
| Al menos 250.000 eventos | CUMPLIDO | 12.296.013 eventos validos en `Gene` | Mantener evidencia del conteo |
| Estrategia para escalar 10x | PENDIENTE | No ejecutada | Definir generador/replicacion controlada y prueba |
| Ingesta batch | CUMPLIDO | `ingestion/ingesta_xm.py` | Evidencia de reejecucion |
| Streaming solo si se justifica | CUMPLIDO EN DISENO | Kafka esta desplegado pero no se usa como flujo principal | Explicar decision en informe |
| HDFS organizado por zonas | PARCIAL | RAW, CURATED y QUALITY operativas | Materializar ANALYTICS final |
| Spark DataFrames o SQL | CUMPLIDO/PARCIAL | DataFrames usados en pipeline | Ejecutar y documentar Spark SQL final |
| Minimo 4 reglas de calidad | CUMPLIDO | R1 completitud, R2 integridad numerica, R3 rango, R4 duplicado | Agregar resumen global por metrica |
| Filas rechazadas/corregidas | CUMPLIDO | `/quality/rejected`; catalogo curado con trazabilidad | Revisar si se materializa `/corrected` |
| Job Spark con particiones | CUMPLIDO | Escritura particionada por `year` | Capturar Spark UI |
| Operacion con shuffle | PARCIAL | GroupBy/window/join generan shuffle potencial | Capturar plan/stages/shuffle real |
| Salida ANALYTICS | PENDIENTE | Zona definida | Materializar resultados finales |
| 2 consultas negocio + 1 tecnica Entrega 2 | PENDIENTE | Analisis preparatorios disponibles | Formalizar consultas en notebook |
| Minimo 5 consultas final | PENDIENTE | Preguntas definidas | Ejecutar, verificar e interpretar |
| Consulta de calidad/trazabilidad | PENDIENTE DE FORMALIZAR | Datos y reglas ya disponibles | Crear consulta final |
| Consulta temporal | PENDIENTE | Datos diarios/horarios disponibles | Implementar |
| Consulta que combine 2 fuentes | PENDIENTE | Join Gene-catalogo realizado, pero falta requisito de tipos de fuente | Resolver segunda fuente y consulta |
| Agregacion segmentada | PENDIENTE DE FORMALIZAR | Posible por embalse/rio/recurso | Implementar |
| Indicador ligado a decision | PENDIENTE | Preguntas definidas | Definir formula e interpretacion |
| Resultado conocido/controlado | PENDIENTE | Validaciones de conteos existentes | Crear dataset pequeno con resultado esperado |
| Desempeno basico | PENDIENTE | Sin tiempos consolidados | Medir tiempos y volumen |
| Fallo/recuperacion o reejecucion | PARCIAL | Ingesta omite archivos existentes | Ejecutar prueba controlada y documentar |
| Prueba a dos volumenes | PENDIENTE | No ejecutada | Dataset controlado vs ampliado |
| Analisis de particiones/shuffle/tiempo/almacenamiento | PENDIENTE | Particion year implementada | Medir y registrar |
| README reproducible | EN ACTUALIZACION | Documento preparado | Probarlo en equipo limpio |
| Diccionario de datos | EN ACTUALIZACION | Documento preparado | Completar unidades confirmadas |
| Plan/matriz de pruebas | EN ACTUALIZACION | Documento preparado | Ejecutar pruebas pendientes |
| Informe final | PENDIENTE | Estructura preparada | Completar con consultas/evidencias |
| Declaracion de uso de IA | PREPARADO | Texto incluido | Ajustar al uso real del equipo |
