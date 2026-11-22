import json
import time
from datetime import date, timedelta
from pathlib import PurePosixPath
from urllib.parse import quote

import requests


# ============================================================
# CONFIGURACION GENERAL
# ============================================================

FECHA_INICIO = date(2020, 1, 1)
FECHA_FIN = date(2026, 11, 19)

DIAS_POR_LLAMADA = 30

API_BASE = "https://servapibi.xm.com.co"
WEBHDFS_BASE = "http://namenode:9870/webhdfs/v1"

HDFS_RAW_BASE = "/proyecto_xm/raw/xm"

PAUSA_ENTRE_LLAMADAS = 0.5
MAX_REINTENTOS = 4


# ============================================================
# METRICAS DEL PROYECTO
# ============================================================

METRICAS = [
    {
        "metric_id": "PorcVoluUtilDiar",
        "entity": "Embalse",
        "endpoint": "daily",
    },
    {
        "metric_id": "VoluUtilDiarMasa",
        "entity": "Embalse",
        "endpoint": "daily",
    },
    {
        "metric_id": "CapaUtilDiarMasa",
        "entity": "Embalse",
        "endpoint": "daily",
    },
    {
        "metric_id": "AporCaudal",
        "entity": "Rio",
        "endpoint": "daily",
    },
    {
        "metric_id": "AporCaudalMediHist",
        "entity": "Rio",
        "endpoint": "daily",
    },
    {
        "metric_id": "PorcApor",
        "entity": "Rio",
        "endpoint": "daily",
    },
    {
        "metric_id": "Gene",
        "entity": "Recurso",
        "endpoint": "hourly",
    },
    {
        "metric_id": "PrecBolsNaci",
        "entity": "Sistema",
        "endpoint": "hourly",
    },
]


CATALOGOS = [
    "ListadoEmbalses",
    "ListadoRios",
    "ListadoRecursos",
]


session = requests.Session()


# ============================================================
# GENERAR VENTANAS DE MAXIMO 30 DIAS
# ============================================================

def generar_ventanas(fecha_inicio, fecha_fin):
    actual = fecha_inicio

    while actual <= fecha_fin:

        fin_anio = date(actual.year, 12, 31)

        fin = min(
            actual + timedelta(days=DIAS_POR_LLAMADA - 1),
            fin_anio,
            fecha_fin,
        )

        yield actual, fin

        actual = fin + timedelta(days=1)


# ============================================================
# LLAMADA A LA API XM CON REINTENTOS
# ============================================================

def consultar_xm(endpoint, payload):

    url = f"{API_BASE}/{endpoint}"

    for intento in range(1, MAX_REINTENTOS + 1):

        try:

            respuesta = session.post(
                url,
                json=payload,
                timeout=120,
            )

            respuesta.raise_for_status()

            datos = respuesta.json()

            if "Items" not in datos:
                raise RuntimeError(
                    f"Respuesta XM sin Items: {datos}"
                )

            return datos

        except Exception as error:

            print(
                f"   ERROR intento {intento}/{MAX_REINTENTOS}: "
                f"{error}"
            )

            if intento == MAX_REINTENTOS:
                raise

            time.sleep(2 * intento)


# ============================================================
# FUNCIONES WEBHDFS
# ============================================================

def hdfs_mkdir(ruta):

    url = (
        WEBHDFS_BASE
        + quote(ruta, safe="/")
    )

    respuesta = session.put(
        url,
        params={"op": "MKDIRS"},
        timeout=60,
    )

    respuesta.raise_for_status()


def hdfs_existe(ruta):

    url = (
        WEBHDFS_BASE
        + quote(ruta, safe="/")
    )

    respuesta = session.get(
        url,
        params={"op": "GETFILESTATUS"},
        allow_redirects=False,
        timeout=60,
    )

    if respuesta.status_code == 200:
        return True

    if respuesta.status_code == 404:
        return False

    respuesta.raise_for_status()


def hdfs_guardar_json(ruta, datos):

    contenido = json.dumps(
        datos,
        ensure_ascii=False,
    ).encode("utf-8")

    directorio = str(
        PurePosixPath(ruta).parent
    )

    hdfs_mkdir(directorio)

    url = (
        WEBHDFS_BASE
        + quote(ruta, safe="/")
    )

    respuesta = session.put(
        url,
        params={
            "op": "CREATE",
            "overwrite": "true",
        },
        allow_redirects=False,
        timeout=60,
    )

    if respuesta.status_code != 307:
        respuesta.raise_for_status()

    destino = respuesta.headers["Location"]

    respuesta_datanode = session.put(
        destino,
        data=contenido,
        headers={
            "Content-Type":
                "application/json; charset=utf-8"
        },
        timeout=180,
    )

    respuesta_datanode.raise_for_status()

    return len(contenido)


# ============================================================
# DESCARGAR CATALOGOS
# ============================================================

def descargar_catalogos():

    print("\n=== CATALOGOS ===")

    for catalogo in CATALOGOS:

        ruta = (
            f"{HDFS_RAW_BASE}/catalogos/"
            f"{catalogo}.json"
        )

        if hdfs_existe(ruta):
            print(f"SKIP {catalogo}: ya existe")
            continue

        datos = consultar_xm(
            "lists",
            {"MetricId": catalogo},
        )

        bytes_guardados = hdfs_guardar_json(
            ruta,
            datos,
        )

        print(
            f"OK {catalogo} "
            f"({bytes_guardados:,} bytes)"
        )


# ============================================================
# DESCARGAR SERIES HISTORICAS
# ============================================================

def descargar_metricas():

    print("\n=== SERIES HISTORICAS XM ===")

    for metrica in METRICAS:

        metric_id = metrica["metric_id"]
        entity = metrica["entity"]
        endpoint = metrica["endpoint"]

        print(
            f"\n### {metric_id} / {entity} "
            f"({endpoint})"
        )

        for inicio, fin in generar_ventanas(
            FECHA_INICIO,
            FECHA_FIN,
        ):

            nombre_archivo = (
                f"{metric_id}_"
                f"{inicio.isoformat()}_"
                f"{fin.isoformat()}.json"
            )

            ruta_hdfs = (
                f"{HDFS_RAW_BASE}/"
                f"{metric_id}/"
                f"year={inicio.year}/"
                f"{nombre_archivo}"
            )

            if hdfs_existe(ruta_hdfs):

                print(
                    f"SKIP "
                    f"{inicio} → {fin}"
                )

                continue

            payload = {
                "MetricId": metric_id,
                "StartDate": inicio.isoformat(),
                "EndDate": fin.isoformat(),
                "Entity": entity,
                "Filter": [],
            }

            print(
                f"DESCARGANDO "
                f"{inicio} → {fin}"
            )

            datos = consultar_xm(
                endpoint,
                payload,
            )

            bytes_guardados = hdfs_guardar_json(
                ruta_hdfs,
                datos,
            )

            items = len(
                datos.get("Items", [])
            )

            print(
                f"   OK "
                f"Items={items:,} "
                f"Bytes={bytes_guardados:,}"
            )

            time.sleep(
                PAUSA_ENTRE_LLAMADAS
            )


# ============================================================
# MAIN
# ============================================================

if __name__ == "__main__":

    print(
        "PERIODO:",
        FECHA_INICIO,
        "→",
        FECHA_FIN,
    )

    descargar_catalogos()

    descargar_metricas()

    print("\nINGESTA FINALIZADA")
