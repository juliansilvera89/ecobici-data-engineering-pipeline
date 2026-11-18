# Databricks notebook source
df_silver = spark.table(
    "micromobility.streaming.silver_stations"
)

display(df_silver.limit(10))

# COMMAND ----------

#OCUPACIÓN

from pyspark.sql.functions import col, round, try_divide

df_gold = (
    df_silver
    .withColumn(
        "occupancy_pct",
        round(
            try_divide(
                col("free_bikes"),
                col("free_bikes") + col("empty_slots")
            ) * 100,
            2
        )
    )
)

display(
    df_gold.select(
        "id",
        "name",
        "free_bikes",
        "empty_slots",
        "occupancy_pct"
    ).limit(10)
)

# COMMAND ----------

# DISPONIBILIDAD

from pyspark.sql.functions import when

df_gold = df_gold.withColumn(
    "availability_status",
    when(col("occupancy_pct") < 20, "Baja")
    .when(col("occupancy_pct") <= 70, "Media")
    .otherwise("Alta")
)

display(
    df_gold.select(
        "id",
        "name",
        "occupancy_pct",
        "availability_status"
    ).limit(10)
)

# COMMAND ----------

# CANTIDAD POR ESTACIÓN

df_gold = df_gold.withColumn(
    "total_slots",
    col("free_bikes") + col("empty_slots")
)

display(
    df_gold.select(
        "id",
        "name",
        "free_bikes",
        "empty_slots",
        "total_slots",
        "occupancy_pct",
        "availability_status"
    ).limit(10)
)

# COMMAND ----------

from pyspark.sql.functions import min, max, countDistinct

df_silver = spark.table(
    "micromobility.streaming.silver_stations"
)

display(
    df_silver.select(
        countDistinct("id").alias("estaciones"),
        countDistinct("timestamp").alias("snapshots"),
        min("timestamp").alias("primer_snapshot"),
        max("timestamp").alias("ultimo_snapshot")
    )
)

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import col, lag, round, unix_timestamp

df_silver = spark.table(
    "micromobility.streaming.silver_stations"
)

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
)

# COMMAND ----------

df_changes = df_changes.withColumn(
    "minutes_between_snapshots",
    round(
        (
            unix_timestamp("timestamp")
            - unix_timestamp("previous_timestamp")
        ) / 60,
        2
    )
)

# COMMAND ----------

df_changes = df_changes.withColumn(
    "bikes_change_per_hour",
    round(
        col("bike_change") * 60
        / col("minutes_between_snapshots"),
        2
    )
)

# COMMAND ----------

df_changes_valid = df_changes.filter(
    col("previous_free_bikes").isNotNull()
    & (col("minutes_between_snapshots") > 0)
)

display(
    df_changes_valid
    .select(
        "name",
        "timestamp",
        "previous_free_bikes",
        "free_bikes",
        "bike_change",
        "minutes_between_snapshots",
        "bikes_change_per_hour"
    )
    .orderBy(col("bike_change").asc())
    .limit(10)
)

# COMMAND ----------

display(
    df_changes_valid
    .filter(
        (col("bike_change") <= -50)
        & (col("minutes_between_snapshots") <= 20)
    )
    .select(
        "name",
        "timestamp",
        "previous_free_bikes",
        "free_bikes",
        "bike_change",
        "minutes_between_snapshots"
    )
    .orderBy("timestamp")
)

# COMMAND ----------

from pyspark.sql.functions import to_date, count, countDistinct

display(
    df_changes_valid
    .filter(
        (col("bike_change") <= -50)
        & (col("minutes_between_snapshots") <= 20)
    )
    .withColumn("fecha", to_date("timestamp"))
    .groupBy("fecha")
    .agg(
        count("*").alias("caidas_detectadas"),
        countDistinct("id").alias("estaciones_afectadas")
    )
    .orderBy("fecha")
)

# COMMAND ----------

(
    df_gold.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(
        "micromobility.streaming.gold_station_analytics"
    )
)

print("Tabla gold_station_analytics guardada correctamente.")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     COUNT(*) AS total_registros,
# MAGIC     COUNT(DISTINCT id) AS estaciones_distintas,
# MAGIC     AVG(occupancy_pct) AS ocupacion_promedio
# MAGIC FROM micromobility.streaming.gold_station_analytics;