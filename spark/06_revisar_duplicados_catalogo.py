from pyspark.sql import SparkSession
from pyspark.sql import functions as F


spark = (
    SparkSession.builder
    .appName("RevisarDuplicadosCatalogo")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


RUTA = (
    "hdfs://namenode:9000/"
    "proyecto_xm/raw/xm/catalogos/"
    "ListadoRecursos.json"
)


raw = (
    spark.read
    .option("multiline", "true")
    .json(RUTA)
)


catalogo = (
    raw
    .select(
        F.explode("Items").alias("item")
    )
    .select(
        F.explode("item.ListEntities").alias("recurso")
    )
    .select(
        F.col("recurso.Values.Code").alias("codigo"),
        F.col("recurso.Values.Name").alias("nombre"),
        F.col("recurso.Values.Type").alias("tipo"),
        F.col("recurso.Values.EnerSource").alias("fuente_energia"),
        F.col("recurso.Values.CompanyCode").alias("empresa"),
        F.col("recurso.Values.State").alias("estado"),
        F.col("recurso.Values.Disp").alias("despacho"),
        F.col("recurso.Values.RecType").alias("tipo_recurso"),
        F.col("recurso.Values.OperStartdate").alias("fecha_operacion")
    )
)


duplicados = (
    catalogo
    .groupBy("codigo")
    .count()
    .filter(F.col("count") > 1)
    .select("codigo")
)


detalle = (
    catalogo
    .join(
        duplicados,
        "codigo",
        "inner"
    )
    .orderBy("codigo")
)


print("\n" + "=" * 70)
print("CODIGOS DUPLICADOS EN LISTADORECURSOS")
print("=" * 70)

detalle.show(
    50,
    truncate=False
)


spark.stop()
