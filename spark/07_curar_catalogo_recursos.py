from pyspark.sql import SparkSession
from pyspark.sql import functions as F


spark = (
    SparkSession.builder
    .appName("CurarCatalogoRecursos")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


RAW = (
    "hdfs://namenode:9000/"
    "proyecto_xm/raw/xm/catalogos/"
    "ListadoRecursos.json"
)

CURATED = (
    "hdfs://namenode:9000/"
    "proyecto_xm/curated/catalogos/recursos"
)

QUALITY = (
    "hdfs://namenode:9000/"
    "proyecto_xm/quality/rejected/catalogos/"
    "ListadoRecursos"
)


# ============================================================
# LEER CATALOGO RAW
# ============================================================

raw = (
    spark.read
    .option("multiline", "true")
    .json(RAW)
)


catalogo = (
    raw
    .select(
        F.explode("Items").alias("item")
    )
    .select(
        F.col("item.Date").alias("fecha_catalogo"),
        F.explode("item.ListEntities").alias("recurso")
    )
    .select(
        F.trim(
            F.col("recurso.Values.Code")
        ).alias("codigo"),

        F.trim(
            F.col("recurso.Values.Name")
        ).alias("nombre"),

        F.trim(
            F.col("recurso.Values.Type")
        ).alias("tipo"),

        F.trim(
            F.col("recurso.Values.EnerSource")
        ).alias("fuente_energia"),

        F.trim(
            F.col("recurso.Values.CompanyCode")
        ).alias("codigo_empresa"),

        F.trim(
            F.col("recurso.Values.State")
        ).alias("estado"),

        F.trim(
            F.col("recurso.Values.Disp")
        ).alias("despacho"),

        F.trim(
            F.col("recurso.Values.RecType")
        ).alias("tipo_recurso"),

        F.col(
            "recurso.Values.OperStartdate"
        ).alias("fecha_operacion"),

        "fecha_catalogo"
    )
)


# ============================================================
# DESCARTAR CODIGOS VACIOS PARA EL CATALOGO CURATED
# ============================================================

validos = catalogo.filter(
    F.col("codigo").isNotNull()
    & (F.length("codigo") > 0)
)


# ============================================================
# AGRUPAR POR CODIGO
# ============================================================

resumen = (
    validos
    .groupBy("codigo")
    .agg(
        F.count("*").alias(
            "cantidad_registros_catalogo"
        ),

        F.sort_array(
            F.collect_set("nombre")
        ).alias("nombres_catalogo"),

        F.sort_array(
            F.collect_set("tipo")
        ).alias("tipos_catalogo"),

        F.sort_array(
            F.collect_set("fuente_energia")
        ).alias("fuentes_catalogo"),

        F.sort_array(
            F.collect_set("codigo_empresa")
        ).alias("empresas_catalogo"),

        F.sort_array(
            F.collect_set("estado")
        ).alias("estados_catalogo"),

        F.sort_array(
            F.collect_set("despacho")
        ).alias("despachos_catalogo"),

        F.sort_array(
            F.collect_set("tipo_recurso")
        ).alias("tipos_recurso_catalogo"),

        F.sort_array(
            F.collect_set("fecha_operacion")
        ).alias("fechas_operacion_catalogo"),

        F.max("fecha_catalogo").alias(
            "fecha_catalogo"
        )
    )
)


# ============================================================
# FUNCION:
# SOLO TOMAR UN VALOR SI EL ATRIBUTO ES UNICO
# ============================================================

def valor_unico(columna):

    return F.when(
        F.size(F.col(columna)) == 1,
        F.element_at(
            F.col(columna),
            1
        )
    )


# ============================================================
# CREAR CATALOGO CURATED
# ============================================================

curated = (
    resumen

    # Para nombre conservamos todas las variantes
    # y usamos una referencia deterministica.
    .withColumn(
        "nombre_referencia",
        F.element_at(
            F.col("nombres_catalogo"),
            1
        )
    )

    .withColumn(
        "tipo",
        valor_unico("tipos_catalogo")
    )

    .withColumn(
        "fuente_energia",
        valor_unico("fuentes_catalogo")
    )

    .withColumn(
        "codigo_empresa",
        valor_unico("empresas_catalogo")
    )

    .withColumn(
        "estado",
        valor_unico("estados_catalogo")
    )

    .withColumn(
        "despacho",
        valor_unico("despachos_catalogo")
    )

    .withColumn(
        "tipo_recurso",
        valor_unico(
            "tipos_recurso_catalogo"
        )
    )

    .withColumn(
        "fecha_operacion",
        valor_unico(
            "fechas_operacion_catalogo"
        )
    )

    # Indicadores de calidad
    .withColumn(
        "duplicado_codigo",
        F.col("cantidad_registros_catalogo") > 1
    )

    .withColumn(
        "conflicto_nombre",
        F.size(
            F.col("nombres_catalogo")
        ) > 1
    )

    .withColumn(
        "conflicto_otros_atributos",
        (
            F.size(F.col("tipos_catalogo")) > 1
        )
        |
        (
            F.size(F.col("fuentes_catalogo")) > 1
        )
        |
        (
            F.size(F.col("empresas_catalogo")) > 1
        )
        |
        (
            F.size(F.col("estados_catalogo")) > 1
        )
        |
        (
            F.size(F.col("despachos_catalogo")) > 1
        )
        |
        (
            F.size(
                F.col("tipos_recurso_catalogo")
            ) > 1
        )
        |
        (
            F.size(
                F.col("fechas_operacion_catalogo")
            ) > 1
        )
    )
)


# ============================================================
# REGISTROS QUE REQUIEREN REVISION DE CALIDAD
# ============================================================

problemas = curated.filter(
    F.col("duplicado_codigo")
    | F.col("conflicto_nombre")
    | F.col("conflicto_otros_atributos")
)


# ============================================================
# CONTEOS
# ============================================================

total_raw = catalogo.count()
total_curated = curated.count()

duplicados = (
    curated
    .filter("duplicado_codigo")
    .count()
)

conflicto_nombre = (
    curated
    .filter("conflicto_nombre")
    .count()
)

conflicto_atributos = (
    curated
    .filter("conflicto_otros_atributos")
    .count()
)


print("\n" + "=" * 70)
print("CURACION DEL CATALOGO DE RECURSOS")
print("=" * 70)

print(
    f"Registros RAW:                "
    f"{total_raw:,}"
)

print(
    f"Codigos unicos CURATED:       "
    f"{total_curated:,}"
)

print(
    f"Codigos con duplicidad:       "
    f"{duplicados:,}"
)

print(
    f"Codigos con conflicto nombre: "
    f"{conflicto_nombre:,}"
)

print(
    f"Conflicto otros atributos:    "
    f"{conflicto_atributos:,}"
)


# ============================================================
# GUARDAR CURATED
# ============================================================

(
    curated
    .write
    .mode("overwrite")
    .parquet(CURATED)
)


# ============================================================
# GUARDAR PROBLEMAS DE CALIDAD
# ============================================================

(
    problemas
    .write
    .mode("overwrite")
    .parquet(QUALITY)
)


print("\nCURATED:")
print(CURATED)

print("\nQUALITY:")
print(QUALITY)


spark.stop()
