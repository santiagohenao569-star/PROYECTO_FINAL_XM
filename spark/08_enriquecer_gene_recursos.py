from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.storagelevel import StorageLevel


# ============================================================
# 1. INICIAR SPARK
# ============================================================

spark = (
    SparkSession.builder
    .appName("EnriquecerGeneConRecursos")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# ============================================================
# 2. RUTAS EN HDFS
# ============================================================

GENE = (
    "hdfs://namenode:9000/"
    "proyecto_xm/curated/Gene"
)

CATALOGO = (
    "hdfs://namenode:9000/"
    "proyecto_xm/curated/catalogos/recursos"
)

SALIDA = (
    "hdfs://namenode:9000/"
    "proyecto_xm/curated/generacion_enriquecida"
)


# ============================================================
# 3. LEER GENE CURATED
# ============================================================

gene = spark.read.parquet(GENE)


# ============================================================
# 4. LEER CATALOGO CURATED
# ============================================================

catalogo = (
    spark.read
    .parquet(CATALOGO)
    .select(
        "codigo",
        "nombre_referencia",
        "tipo",
        "fuente_energia",
        "codigo_empresa",
        "estado",
        "despacho",
        "tipo_recurso",
        "fecha_operacion"
    )
)


# ============================================================
# 5. CONTAR GENE ANTES DEL JOIN
# ============================================================

total_antes = gene.count()


# ============================================================
# 6. LEFT JOIN
# ============================================================

enriquecida = (
    gene.alias("g")
    .join(
        catalogo.alias("c"),
        F.col("g.entidad") == F.col("c.codigo"),
        "left"
    )
    .select(
        F.col("g.metrica"),
        F.col("g.fecha"),
        F.col("g.hora"),
        F.col("g.entidad").alias("codigo_recurso"),
        F.col("g.valor").alias("generacion"),
        F.col("g.archivo_origen"),
        F.col("g.year"),

        F.col("c.nombre_referencia")
        .alias("nombre_recurso"),

        F.col("c.tipo"),
        F.col("c.fuente_energia"),
        F.col("c.codigo_empresa"),
        F.col("c.estado"),
        F.col("c.despacho"),
        F.col("c.tipo_recurso"),
        F.col("c.fecha_operacion"),

        F.col("c.codigo")
        .isNotNull()
        .alias("correspondencia_catalogo")
    )
)


# ============================================================
# 7. IDENTIFICAR RECURSOS HIDRAULICOS
# ============================================================

enriquecida = (
    enriquecida
    .withColumn(
        "es_hidraulico",
        F.when(
            ~F.col("correspondencia_catalogo"),
            F.lit(None).cast("boolean")
        )
        .otherwise(
            (
                F.upper(F.trim(F.col("tipo")))
                == "HIDRAULICA"
            )
            &
            (
                F.upper(F.trim(F.col("fuente_energia")))
                == "AGUA"
            )
        )
    )
)


# Evita recalcular el JOIN para cada conteo
enriquecida.persist(StorageLevel.MEMORY_AND_DISK)


# ============================================================
# 8. VALIDACIONES
# ============================================================

total_despues = enriquecida.count()

eventos_sin_catalogo = (
    enriquecida
    .filter(
        F.col("correspondencia_catalogo") == False
    )
    .count()
)

recursos_sin_catalogo = (
    enriquecida
    .filter(
        F.col("correspondencia_catalogo") == False
    )
    .select("codigo_recurso")
    .distinct()
    .count()
)

eventos_hidraulicos = (
    enriquecida
    .filter(
        F.col("es_hidraulico") == True
    )
    .count()
)

recursos_hidraulicos = (
    enriquecida
    .filter(
        F.col("es_hidraulico") == True
    )
    .select("codigo_recurso")
    .distinct()
    .count()
)


# ============================================================
# 9. MOSTRAR RESULTADOS
# ============================================================

print("\n" + "=" * 70)
print("ENRIQUECIMIENTO GENE + LISTADORECURSOS")
print("=" * 70)

print(f"Filas antes del JOIN:       {total_antes:,}")
print(f"Filas despues del JOIN:     {total_despues:,}")
print(f"Eventos sin catalogo:       {eventos_sin_catalogo:,}")
print(f"Recursos sin catalogo:      {recursos_sin_catalogo:,}")
print(f"Eventos hidraulicos:        {eventos_hidraulicos:,}")
print(f"Recursos hidraulicos:       {recursos_hidraulicos:,}")


# ============================================================
# 10. CONTROL DE DUPLICACION
# ============================================================

if total_antes != total_despues:

    raise RuntimeError(
        "ERROR: el JOIN cambio la cantidad de filas. "
        "Existe riesgo de duplicacion."
    )


# ============================================================
# 11. GUARDAR RESULTADO CURATED
# ============================================================

(
    enriquecida
    .write
    .mode("overwrite")
    .partitionBy("year")
    .parquet(SALIDA)
)


print("\nJOIN VALIDADO: no se duplicaron filas")
print(f"Salida CURATED: {SALIDA}")


enriquecida.unpersist()

spark.stop()
