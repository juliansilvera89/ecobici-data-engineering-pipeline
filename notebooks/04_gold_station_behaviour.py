# Databricks notebook source
from pyspark.sql.window import Window
from pyspark.sql.functions import col, lag, round, unix_timestamp

# Leer el historial de estaciones
df_silver = spark.table(
    "micromobility.streaming.silver_stations"
)

# Comparar cada estación con su observación anterior
window_station = (
    Window
    .partitionBy("id")
    .orderBy("timestamp")
)

df_changes = (
    df_silver
    .withColumn(
        "previous_free_bikes",
        lag("free_bikes").over(window_station)
    )
    .withColumn(
        "previous_timestamp",
        lag("timestamp").over(window_station)
    )
    .withColumn(
        "bike_change",
        col("free_bikes") - col("previous_free_bikes")
    )
    .withColumn(
        "minutes_between_snapshots",
        round(
            (
                unix_timestamp("timestamp")
                - unix_timestamp("previous_timestamp")
            ) / 60,
            2
        )
    )
)

print("Comparación histórica calculada.")

# COMMAND ----------

from pyspark.sql.functions import when

df_gold_behavior = (
    df_changes
    .filter(
        col("previous_free_bikes").isNotNull()
        & (col("minutes_between_snapshots") > 0)
    )
    .withColumn(
        "bikes_change_per_hour",
        round(
            col("bike_change") * 60
            / col("minutes_between_snapshots"),
            2
        )
    )
    .withColumn(
        "sharp_drop",
        when(
            (col("bike_change") <= -50)
            & (col("minutes_between_snapshots") <= 20),
            True
        ).otherwise(False)
    )
)

print("Métricas de comportamiento calculadas.")

# COMMAND ----------

(
    df_gold_behavior.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(
        "micromobility.streaming.gold_station_behavior"
    )
)

print("Tabla gold_station_behavior guardada correctamente.")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     COUNT(*) AS registros,
# MAGIC     COUNT(DISTINCT id) AS estaciones,
# MAGIC     SUM(CASE WHEN sharp_drop THEN 1 ELSE 0 END) AS caidas_bruscas
# MAGIC FROM micromobility.streaming.gold_station_behavior;