import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


# ============================================================
# CONFIGURACION
# ============================================================

FECHA_INICIO = "2020-01-01"
FECHA_FIN = "2026-11-19"

RAW_BASE = "hdfs://namenode:9000/proyecto_xm/raw/xm"
CURATED_BASE = "hdfs://namenode:9000/proyecto_xm/curated"
REJECTED_BASE = "hdfs://namenode:9000/proyecto_xm/quality/rejected"


METRICAS_DIARIAS = {
    "PorcVoluUtilDiar": "Embalse",
    "VoluUtilDiarMasa": "Embalse",
    "CapaUtilDiarMasa": "Embalse",
    "AporCaudal": "Rio",
    "AporCaudalMediHist": "Rio",
    "PorcApor": "Rio",
}


METRICAS_HORARIAS = {
    "Gene": "Recurso",
    "PrecBolsNaci": "Sistema",
}


# ============================================================
# SPARK
# ============================================================

spark = (
    SparkSession.builder
    .appName("XM_RAW_A_CURATED")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# ============================================================
# LECTURA RAW
# ============================================================

def leer_raw(metrica):

    ruta = f"{RAW_BASE}/{metrica}/year=*/*.json"

    return (
        spark.read
        .option("multiline", "true")
        .json(ruta)
        .withColumn(
            "archivo_origen",
            F.input_file_name()
        )
    )


# ============================================================
# NORMALIZAR METRICAS DIARIAS
# ============================================================

def normalizar_diaria(metrica, tipo_entidad):

    df_raw = leer_raw(metrica)

    df_items = (
        df_raw
        .select(
            "archivo_origen",
            F.explode_outer("Items").alias("item")
        )
    )

    df = (
        df_items
        .select(
            "archivo_origen",
            F.col("item.Date").cast("string").alias("fecha_raw"),
            F.explode_outer(
                "item.DailyEntities"
            ).alias("entidad_obj")
        )
        .select(
            F.lit(metrica).alias("metrica"),
            F.lit(tipo_entidad).alias("tipo_entidad"),
            "archivo_origen",
            "fecha_raw",
            F.col("entidad_obj.Name")
             .cast("string")
             .alias("entidad_raw"),
            F.col("entidad_obj.Value")
             .cast("string")
             .alias("valor_raw")
        )
    )

    return (
        df
        .withColumn(
            "fecha",
            F.to_date("fecha_raw")
        )
        .withColumn(
            "hora",
            F.lit(None).cast("int")
        )
        .withColumn(
            "entidad",
            F.trim("entidad_raw")
        )
        .withColumn(
            "valor",
            F.col("valor_raw").cast("double")
        )
    )


# ============================================================
# NORMALIZAR METRICAS HORARIAS
# ============================================================

def normalizar_horaria(metrica, tipo_entidad):

    df_raw = leer_raw(metrica)

    df_items = (
        df_raw
        .select(
            "archivo_origen",
            F.explode_outer("Items").alias("item")
        )
    )

    df_entidades = (
        df_items
        .select(
            "archivo_origen",
            F.col("item.Date").cast("string").alias("fecha_raw"),
            F.explode_outer(
                "item.HourlyEntities"
            ).alias("entidad_obj")
        )
        .withColumn(
            "metrica",
            F.lit(metrica)
        )
        .withColumn(
            "tipo_entidad",
            F.lit(tipo_entidad)
        )
    )

    pares_horas = ", ".join(
        [
            f"'{hora:02d}', entidad_obj.Values.Hour{hora:02d}"
            for hora in range(1, 25)
        ]
    )

    df = df_entidades.selectExpr(
        "metrica",
        "tipo_entidad",
        "archivo_origen",
        "fecha_raw",
        "entidad_obj.Values.code as entidad_raw",
        f"stack(24, {pares_horas}) as (hora_raw, valor_raw)"
    )

    return (
        df
        .withColumn(
            "fecha",
            F.to_date("fecha_raw")
        )
        .withColumn(
            "hora",
            F.col("hora_raw").cast("int")
        )
        .withColumn(
            "entidad",
            F.trim("entidad_raw")
        )
        .withColumn(
            "valor",
            F.col("valor_raw").cast("double")
        )
    )


# ============================================================
# REGLAS DE CALIDAD
# ============================================================

def aplicar_calidad(df, es_horaria):

    # R1: completitud
    r1_completitud = (
        F.col("fecha").isNull()
        | F.col("entidad").isNull()
        | (F.length(F.col("entidad")) == 0)
        | F.col("valor_raw").isNull()
        | (
            F.length(
                F.trim(
                    F.col("valor_raw").cast("string")
                )
            ) == 0
        )
    )

    # R2: integridad numerica
    r2_integridad = (
        F.col("valor_raw").isNotNull()
        & (
            F.length(
                F.trim(
                    F.col("valor_raw").cast("string")
                )
            ) > 0
        )
        & F.col("valor").isNull()
    )

    # R3: rango temporal
    r3_rango = (
        (
            F.col("fecha")
            < F.lit(FECHA_INICIO).cast("date")
        )
        |
        (
            F.col("fecha")
            > F.lit(FECHA_FIN).cast("date")
        )
    )

    if es_horaria:

        r3_rango = (
            r3_rango
            | (F.col("hora") < 1)
            | (F.col("hora") > 24)
            | F.col("hora").isNull()
        )

        ventana = Window.partitionBy(
            "metrica",
            "fecha",
            "entidad",
            "hora"
        )

    else:

        ventana = Window.partitionBy(
            "metrica",
            "fecha",
            "entidad"
        )

    # R4: duplicados
    df = df.withColumn(
        "conteo_clave",
        F.count("*").over(ventana)
    )

    r4_duplicado = (
        F.col("conteo_clave") > 1
    )

    return (
        df
        .withColumn(
            "error_calidad",
            F.concat_ws(
                " | ",
                F.when(
                    r1_completitud,
                    "R1_COMPLETITUD"
                ),
                F.when(
                    r2_integridad,
                    "R2_INTEGRIDAD_NUMERICA"
                ),
                F.when(
                    r3_rango,
                    "R3_RANGO"
                ),
                F.when(
                    r4_duplicado,
                    "R4_DUPLICADO"
                )
            )
        )
        .withColumn(
            "year",
            F.year("fecha")
        )
    )


# ============================================================
# PROCESAR UNA METRICA
# ============================================================

def procesar_metrica(
    metrica,
    tipo_entidad,
    es_horaria
):

    print("\n" + "=" * 70)
    print(f"PROCESANDO: {metrica}")
    print("=" * 70)

    if es_horaria:
        df = normalizar_horaria(
            metrica,
            tipo_entidad
        )
    else:
        df = normalizar_diaria(
            metrica,
            tipo_entidad
        )

    df = aplicar_calidad(
        df,
        es_horaria
    )

    df_validos = (
        df
        .filter(
            F.col("error_calidad") == ""
        )
        .select(
            "metrica",
            "fecha",
            "hora",
            "tipo_entidad",
            "entidad",
            "valor",
            "archivo_origen",
            "year"
        )
    )

    df_rechazados = (
        df
        .filter(
            F.col("error_calidad") != ""
        )
    )

    total = df.count()
    rechazados = df_rechazados.count()
    validos = total - rechazados

    print(f"Registros normalizados: {total:,}")
    print(f"Registros validos:      {validos:,}")
    print(f"Registros rechazados:   {rechazados:,}")

    ruta_curated = (
        f"{CURATED_BASE}/{metrica}"
    )

    ruta_rejected = (
        f"{REJECTED_BASE}/{metrica}"
    )

    (
        df_validos
        .repartition("year")
        .write
        .mode("overwrite")
        .partitionBy("year")
        .parquet(ruta_curated)
    )

    (
        df_rechazados
        .write
        .mode("overwrite")
        .parquet(ruta_rejected)
    )

    print(
        f"CURATED:  {ruta_curated}"
    )

    print(
        f"REJECTED: {ruta_rejected}"
    )


# ============================================================
# SELECCION DE METRICAS
# ============================================================

todas_metricas = {
    **METRICAS_DIARIAS,
    **METRICAS_HORARIAS,
}

if len(sys.argv) > 1:
    metricas_objetivo = sys.argv[1:]
else:
    metricas_objetivo = list(
        todas_metricas.keys()
    )


# ============================================================
# EJECUCION
# ============================================================

for metrica in metricas_objetivo:

    if metrica in METRICAS_DIARIAS:

        procesar_metrica(
            metrica,
            METRICAS_DIARIAS[metrica],
            False
        )

    elif metrica in METRICAS_HORARIAS:

        procesar_metrica(
            metrica,
            METRICAS_HORARIAS[metrica],
            True
        )

    else:

        print(
            f"METRICA DESCONOCIDA: {metrica}"
        )


spark.stop()
