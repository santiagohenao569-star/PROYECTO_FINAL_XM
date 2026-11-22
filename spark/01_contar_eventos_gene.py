from pyspark.sql import SparkSession
from pyspark.sql import functions as F


# ============================================================
# CREAR SESION SPARK
# ============================================================

spark = (
    SparkSession.builder
    .appName("ConteoEventosGene")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# ============================================================
# LEER TODOS LOS JSON RAW DE GENERACION
# ============================================================

ruta_gene = (
    "hdfs://namenode:9000/"
    "proyecto_xm/raw/xm/Gene/year=*/*.json"
)

df_raw = (
    spark.read
    .option("multiline", "true")
    .json(ruta_gene)
)


# ============================================================
# EXTRAER ITEMS
# ============================================================

df_items = df_raw.select(
    F.explode("Items").alias("item")
)


# ============================================================
# EXTRAER RECURSOS
# ============================================================

df_recursos = df_items.select(
    F.col("item.Date").alias("fecha"),
    F.explode("item.HourlyEntities").alias("entidad")
)


# ============================================================
# CONVERTIR HOUR01 ... HOUR24 EN FILAS
# ============================================================

pares_horas = ", ".join(
    [
        f"'{hora:02d}', entidad.Values.Hour{hora:02d}"
        for hora in range(1, 25)
    ]
)

df_eventos = df_recursos.selectExpr(
    "fecha",
    "entidad.Values.code as recurso",
    f"stack(24, {pares_horas}) as (hora, valor)"
)


# ============================================================
# DESCARTAR VALORES VACIOS
# ============================================================

df_eventos_validos = (
    df_eventos
    .filter(
        F.col("valor").isNotNull()
        & (F.trim(F.col("valor")) != "")
    )
)


# ============================================================
# CONTEOS
# ============================================================

archivos_json = df_raw.count()
items_api = df_items.count()
eventos = df_eventos_validos.count()


print("\n" + "=" * 60)
print("RESULTADOS GENERACION XM")
print("=" * 60)

print(f"Archivos JSON RAW: {archivos_json:,}")
print(f"Items API:         {items_api:,}")
print(f"Eventos horarios:  {eventos:,}")

print("-" * 60)

if eventos >= 250_000:
    print("REQUISITO >= 250.000 EVENTOS: CUMPLIDO")
else:
    print("REQUISITO >= 250.000 EVENTOS: NO CUMPLIDO")

print("=" * 60)


spark.stop()
