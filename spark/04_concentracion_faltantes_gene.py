from pyspark.sql import SparkSession
from pyspark.sql import functions as F


spark = (
    SparkSession.builder
    .appName("ConcentracionFaltantesGene")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


CURATED = (
    "hdfs://namenode:9000/"
    "proyecto_xm/curated/Gene"
)

REJECTED = (
    "hdfs://namenode:9000/"
    "proyecto_xm/quality/rejected/Gene"
)


# ============================================================
# LEER DATOS
# ============================================================

validos = (
    spark.read.parquet(CURATED)
    .select(
        "year",
        "entidad"
    )
)

rechazados = (
    spark.read.parquet(REJECTED)
    .filter(
        F.col("error_calidad").contains(
            "R1_COMPLETITUD"
        )
    )
    .select(
        "year",
        F.col("entidad_raw").alias("entidad")
    )
)


# ============================================================
# RESUMEN POR AÑO
# ============================================================

validos_anio = (
    validos
    .groupBy("year")
    .count()
    .withColumnRenamed(
        "count",
        "validos"
    )
)

rechazados_anio = (
    rechazados
    .groupBy("year")
    .count()
    .withColumnRenamed(
        "count",
        "faltantes"
    )
)

resumen_anio = (
    validos_anio
    .join(
        rechazados_anio,
        "year",
        "outer"
    )
    .fillna(0)
    .withColumn(
        "total_posiciones",
        F.col("validos")
        + F.col("faltantes")
    )
    .withColumn(
        "porcentaje_faltante",
        F.round(
            F.col("faltantes")
            / F.col("total_posiciones")
            * 100,
            2
        )
    )
    .orderBy("year")
)


print("\n" + "=" * 70)
print("FALTANTES DE GENE POR AÑO")
print("=" * 70)

resumen_anio.show(
    20,
    truncate=False
)


# ============================================================
# RESUMEN POR RECURSO
# ============================================================

validos_recurso = (
    validos
    .groupBy("entidad")
    .count()
    .withColumnRenamed(
        "count",
        "validos"
    )
)

rechazados_recurso = (
    rechazados
    .groupBy("entidad")
    .count()
    .withColumnRenamed(
        "count",
        "faltantes"
    )
)

resumen_recurso = (
    validos_recurso
    .join(
        rechazados_recurso,
        "entidad",
        "outer"
    )
    .fillna(0)
    .withColumn(
        "total_posiciones",
        F.col("validos")
        + F.col("faltantes")
    )
    .withColumn(
        "porcentaje_faltante",
        F.round(
            F.col("faltantes")
            / F.col("total_posiciones")
            * 100,
            2
        )
    )
)


print("\n" + "=" * 70)
print("20 RECURSOS CON MAYOR CANTIDAD DE FALTANTES")
print("=" * 70)

(
    resumen_recurso
    .orderBy(
        F.desc("faltantes")
    )
    .show(
        20,
        truncate=False
    )
)


spark.stop()
