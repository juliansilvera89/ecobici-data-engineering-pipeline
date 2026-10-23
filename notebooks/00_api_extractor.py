# Databricks notebook source
import json
import requests

# 1. Endpoint de ECOBICI Buenos Aires
api_url = "http://api.citybik.es/v2/networks/ecobici"

# 2. Petición a la API
response = requests.get(api_url)

if response.status_code == 200:
    json_data = response.json()
    stations_list = json_data["network"]["stations"]
    print(f"Conexión exitosa. Se encontraron {len(stations_list)} estaciones.")

    # 3. Serializar cada diccionario a String JSON
    json_strings = [json.dumps(station) for station in stations_list]

    # 4. Crear DataFrame temporal en Spark sin usar 'sc' ni RDDs
    # Infiere el esquema JSON perfecto automáticamente
    df_strings = spark.createDataFrame([(s,) for s in json_strings], ["json_raw"])
    df_ecobici = spark.read.json(df_strings.select("json_raw").rdd) if False else spark.read.json(
        spark.createDataFrame(json_strings, "string")
    )

    # 5. Visualizar
    display(df_ecobici)

else:
    print(f"Error al conectar con la API: {response.status_code}")

# COMMAND ----------

df_ecobici.printSchema()

# COMMAND ----------

display(df_ecobici.limit(10))

# COMMAND ----------

import json
import requests
from datetime import datetime

API_URL = "http://api.citybik.es/v2/networks/ecobici"

RAW_PATH = "/Volumes/micromobility/streaming/raw_landing"

# Consultar API
response = requests.get(API_URL, timeout=10)
response.raise_for_status()

data = response.json()

# Extraer estaciones
stations = data["network"]["stations"]

print(f"Estaciones obtenidas: {len(stations)}")

# Crear nombre único para cada snapshot
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

output_path = f"{RAW_PATH}/ecobici_{timestamp}.json"

# Guardar JSON
content = "\n".join(
    json.dumps(station, ensure_ascii=False)
    for station in stations
)

dbutils.fs.put(
    output_path,
    content,
    overwrite=False
)

print(f"Snapshot guardado en: {output_path}")

# COMMAND ----------

display(
    spark.read.json(
        "/Volumes/micromobility/streaming/raw_landing/ecobici_20261005_004250.json"
    )
)