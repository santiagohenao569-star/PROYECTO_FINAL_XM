from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = (
    SparkSession.builder
    .appName("ResumenCalidadGene")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

ruta = (
    "hdfs://namenode:9000/"
    "proyecto_xm/quality/rejected/Gene"
)

df = spark.read.parquet(ruta)

print("\n" + "=" * 60)
print("CAUSAS DE RECHAZO - GENE")
print("=" * 60)

(
    df.groupBy("error_calidad")
    .count()
    .orderBy(F.desc("count"))
    .show(truncate=False)
)

print("\nEJEMPLOS DE REGISTROS RECHAZADOS")

df.select(
    "fecha_raw",
    "entidad_raw",
    "hora_raw",
    "valor_raw",
    "error_calidad"
).show(20, truncate=False)

spark.stop()
