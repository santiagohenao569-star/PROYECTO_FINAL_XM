# Plan de evidencias y pantallazos

Objetivo: tomar solo evidencias que demuestren requisitos o resultados. Evitar capturas repetidas que no aporten informacion.

## Convencion de nombres

Guardar en `evidence/` con prefijo numerico:

```text
E01_docker_compose_ps.png
E02_hdfs_datanodes.png
...
```

Si ya existe una captura equivalente, no es necesario repetirla; renombrarla y registrarla es suficiente.

## Evidencias actuales o que conviene conservar

### E01 - Infraestructura activa

Comando:

```bash
docker compose ps
```

Debe verse el conjunto de servicios del proyecto en estado activo.

**Demuestra:** despliegue reproducible e infraestructura activa.

### E02 - HDFS con dos DataNodes

Abrir la UI del NameNode y capturar la seccion donde se observen **2 DataNodes vivos**.

**Demuestra:** almacenamiento distribuido y nodos disponibles.

### E03 - Zonas HDFS

Comando:

```bash
docker exec xm-namenode hdfs dfs -ls /proyecto_xm
```

Debe mostrar las zonas disponibles (RAW/CURATED/QUALITY y ANALYTICS cuando se materialice).

### E04 - Volumen RAW

Comando recomendado:

```bash
docker exec xm-namenode hdfs dfs -count -h /proyecto_xm/raw/xm/*
```

**Demuestra:** archivos, volumen y distribucion por metrica.

### E05 - Conteo Gene

Ejecutar `01_contar_eventos_gene.py` y capturar el bloque final con:

- 89 archivos;
- 662.600 Items;
- 12.296.013 eventos validos.

### E06 - Calidad Gene

Ejecutar `03_resumen_calidad_gene.py` y capturar el bloque donde aparezca `R1_COMPLETITUD = 3.606.387`.

### E07 - Concentracion temporal de faltantes

Ejecutar `04_concentracion_faltantes_gene.py` y capturar la tabla por ano. Es evidencia de perfilamiento; no usarla para afirmar una causa.

### E08 - Integridad referencial

Ejecutar `05_validar_catalogo_recursos.py` y capturar:

- 672 recursos Gene;
- 647 encontrados;
- 25 no encontrados;
- 160 hidraulicos.

### E09 - Catalogo CURATED

Ejecutar `07_curar_catalogo_recursos.py` y capturar:

- 2.522 RAW;
- 2.519 unicos;
- 3 codigos duplicados;
- 1 conflicto de nombre.

### E10 - JOIN Gene + recursos

Ejecutar `08_enriquecer_gene_recursos.py` y capturar exactamente el bloque:

```text
Filas antes del JOIN:       12,296,013
Filas despues del JOIN:     12,296,013
Eventos sin catalogo:       112,246
Recursos sin catalogo:      25
Eventos hidraulicos:        7,277,050
Recursos hidraulicos:       160
```

Esta es una de las evidencias mas importantes del pipeline.

## Evidencias que tomaremos desde ahora

### E11 - Jupyter leyendo CURATED

Cuando iniciemos el notebook, tomar una captura donde se vea:

- la celda que lee `generacion_enriquecida` desde HDFS;
- `printSchema()` o una muestra de 5 filas.

**No es necesario mostrar 12 millones de filas.**

### E12 - Spark Master

Capturar Spark Master con dos Workers registrados.

### E13 - Spark Application / Stages

Durante una consulta que produzca agregacion/JOIN, abrir Spark UI (`4040` si esta publicada) y capturar Jobs/Stages.

### E14 - Resultado conocido

Crear un subconjunto pequeno cuyo resultado pueda comprobarse manualmente. Capturar la entrada y el resultado Spark.

### E15 - Consulta 1 calidad

Captura del SQL y salida resumida.

### E16 - Consulta 2 embalses

Captura del resultado principal y, si aporta valor, grafico temporal.

### E17 - Consulta 3 aportes

Captura del ranking/periodos por debajo de referencia y visualizacion temporal si se usa.

### E18 - Consulta 4 generacion hidraulica

Captura de tabla/grafico que compare generacion entre condiciones hidricas.

### E19 - Consulta 5 precio de bolsa

Captura de tabla/grafico que compare precio entre condiciones hidricas.

### E20 - ANALYTICS en HDFS

Cuando escribamos resultados finales:

```bash
docker exec xm-namenode hdfs dfs -ls -R /proyecto_xm/analytics
```

Capturar rutas/materializaciones finales.

### E21 - Shuffle/plan

Capturar Spark UI o `explain()` donde pueda demostrarse una operacion con intercambio/shuffle. No interpretar solo por el nombre; registrar la consulta que lo genero.

### E22 - Tiempo y tamano

Capturar o registrar tiempos de una ejecucion definida junto al numero de registros/tamano procesado.

### E23 - Escala con dos volumenes

Capturar tabla comparativa:

```text
volumen | registros | tiempo | salida | observaciones
```

### E24 - Estrategia 10x

Capturar ejecucion o evidencia de la estrategia de escalamiento 10x cuando se implemente.

### E25 - Idempotencia

Reejecutar una ingesta/proceso controlado y demostrar que no duplica datos.

### E26 - Recuperacion

Ejecutar la prueba de fallo/reinicio elegida y capturar antes/despues.

### E27 - Git final

Capturar:

```bash
git status
git log --oneline -n 5
```

En la version final debe observarse el repositorio limpio y el commit/tag de entrega.

## Reglas para tomar pantallazos

1. Mostrar el comando o celda y el resultado relevante siempre que sea posible.
2. Evitar capturas gigantes con texto ilegible.
3. No recortar el valor clave que se quiere demostrar.
4. No incluir credenciales, tokens ni informacion personal innecesaria.
5. Usar una evidencia para un argumento concreto; no insertar pantallazos solo por llenar el informe.
6. Registrar el nombre del archivo inmediatamente para no perder trazabilidad.
