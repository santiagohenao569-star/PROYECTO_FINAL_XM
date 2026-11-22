import requests

URL = "https://servapibi.xm.com.co/lists"
PAYLOAD = {"MetricId": "ListadoMetricas"}

respuesta = requests.post(URL, json=PAYLOAD, timeout=60)
respuesta.raise_for_status()

datos = respuesta.json()

metricas = []


def recorrer(objeto):
    if isinstance(objeto, dict):
        if "MetricId" in objeto and "MetricName" in objeto:
            metricas.append(objeto)

        for valor in objeto.values():
            recorrer(valor)

    elif isinstance(objeto, list):
        for elemento in objeto:
            recorrer(elemento)


recorrer(datos)

palabras_clave = [
    "hidro",
    "embalse",
    "rio",
    "río",
    "aporte",
    "caudal",
    "volumen",
    "reserva",
]


for metrica in metricas:
    texto = (
        str(metrica.get("MetricId", "")) + " "
        + str(metrica.get("MetricName", "")) + " "
        + str(metrica.get("MetricDescription", ""))
    ).lower()

    if any(palabra in texto for palabra in palabras_clave):
        print("-" * 100)
        print("MetricId:   ", metrica.get("MetricId"))
        print("MetricName: ", metrica.get("MetricName"))
        print("Entity:     ", metrica.get("Entity"))
        print("Units:      ", metrica.get("MetricUnits"))
        print("Type:       ", metrica.get("Type"))
        print("MaxDays:    ", metrica.get("MaxDays"))
        print("Filter:     ", metrica.get("Filter"))
        print("URL:        ", metrica.get("Url"))
