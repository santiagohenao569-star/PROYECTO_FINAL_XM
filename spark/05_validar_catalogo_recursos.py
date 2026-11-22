from pyspark.sql import SparkSession
from pyspark.sql import functions as F


spark = (
    SparkSession.builder
    .appName("ValidarCatalogoRecursos")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# ============================================================
# RUTAS
# ============================================================

RUTA_CATALOGO = (
    "hdfs://namenode:9000/"
    "proyecto_xm/raw/xm/catalogos/"
    "ListadoRecursos.json"
)

RUTA_GENE = (
    "hdfs://namenode:9000/"
    "proyecto_xm/curated/Gene"
)


# ============================================================
# LEER CATALOGO DE RECURSOS
# ============================================================

raw_catalogo = (
    spark.read
    .option("multiline", "true")
    .json(RUTA_CATALOGO)
)


catalogo = (
    raw_catalogo
    .select(
        F.explode("Items").alias("item")
    )
    .select(
        F.col("item.Date").alias("fecha_catalogo"),
        F.explode("item.ListEntities").alias("recurso")
    )
    .select(
        "fecha_catalogo",
        F.col("recurso.Values.Code").alias("codigo"),
        F.col("recurso.Values.Name").alias("nombre"),
        F.col("recurso.Values.Type").alias("tipo"),
        F.col("recurso.Values.EnerSource").alias("fuente_energia"),
        F.col("recurso.Values.CompanyCode").alias("codigo_empresa"),
        F.col("recurso.Values.OperStartdate").alias("fecha_operacion"),
        F.col("recurso.Values.State").alias("estado"),
        F.col("recurso.Values.Disp").alias("despacho"),
        F.col("recurso.Values.RecType").alias("tipo_recurso")
    )
)


# ============================================================
# PERFILAR CATALOGO
# ============================================================

total_catalogo = catalogo.count()

codigos_distintos = (
    catalogo
    .select("codigo")
    .distinct()
    .count()
)

duplicados = (
    catalogo
    .groupBy("codigo")
    .count()
    .filter(F.col("count") > 1)
)


print("\n" + "=" * 70)
print("CATALOGO DE RECURSOS")
print("=" * 70)

print(f"Registros catalogo: {total_catalogo:,}")
print(f"Codigos distintos:  {codigos_distintos:,}")
print(f"Codigos duplicados: {duplicados.count():,}")


# ============================================================
# IDENTIFICAR RECURSOS HIDRAULICOS
# ============================================================

hidraulicos = (
    catalogo
    .filter(
        (F.upper(F.trim(F.col("tipo"))) == "HIDRAULICA")
        &
        (F.upper(F.trim(F.col("fuente_energia"))) == "AGUA")
    )
    .select("codigo")
    .distinct()
)


print(
    f"Recursos hidraulicos en catalogo: "
    f"{hidraulicos.count():,}"
)


# ============================================================
# RECURSOS PRESENTES EN GENE
# ============================================================

recursos_gene = (
    spark.read
    .parquet(RUTA_GENE)
    .select(
        F.col("entidad").alias("codigo")
    )
    .distinct()
)


total_gene = recursos_gene.count()


# ============================================================
# VALIDAR INTEGRIDAD REFERENCIAL
# ============================================================

codigos_catalogo = (
    catalogo
    .select("codigo")
    .filter(F.col("codigo").isNotNull())
    .distinct()
)


coinciden = (
    recursos_gene
    .join(
        codigos_catalogo,
        "codigo",
        "inner"
    )
)


no_coinciden = (
    recursos_gene
    .join(
        codigos_catalogo,
        "codigo",
        "left_anti"
    )
)


hidraulicos_gene = (
    recursos_gene
    .join(
        hidraulicos,
        "codigo",
        "inner"
    )
)


print("\n" + "=" * 70)
print("INTEGRIDAD REFERENCIAL GENE vs LISTADORECURSOS")
print("=" * 70)

print(
    f"Recursos distintos en Gene: "
    f"{total_gene:,}"
)

print(
    f"Recursos encontrados en catalogo: "
    f"{coinciden.count():,}"
)

print(
    f"Recursos NO encontrados en catalogo: "
    f"{no_coinciden.count():,}"
)

print(
    f"Recursos hidraulicos presentes en Gene: "
    f"{hidraulicos_gene.count():,}"
)


print("\nCODIGOS DE GENE NO ENCONTRADOS EN CATALOGO")

no_coinciden.show(
    30,
    truncate=False
)


spark.stop()
