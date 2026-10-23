# Databricks notebook source
from pyspark.sql.functions import col

df_silver = spark.table(
    "micromobility.streaming.silver_stations"
)

invalid_condition = (
    col("id").isNull()
    | col("timestamp").isNull()
    | (col("free_bikes") < 0)
    | (col("empty_slots") < 0)
    | col("latitude").isNull()
    | col("longitude").isNull()
    | ~col("latitude").between(-90, 90)
    | ~col("longitude").between(-180, 180)
)

df_invalid = df_silver.filter(invalid_condition)
df_valid = df_silver.filter(~invalid_condition)

print(f"Registros totales: {df_silver.count()}")
print(f"Registros válidos: {df_valid.count()}")
print(f"Registros sospechosos: {df_invalid.count()}")

# COMMAND ----------

from pyspark.sql.functions import count, sum as spark_sum

# Registros duplicados por estación y timestamp
df_duplicates = (
    df_silver
    .groupBy("id", "timestamp")
    .agg(count("*").alias("cantidad"))
    .filter(col("cantidad") > 1)
)

# Valores que requieren revisión
df_capacity_issues = df_silver.filter(
    col("free_bikes").isNull()
    | col("empty_slots").isNull()
    | (
        (col("free_bikes") + col("empty_slots")) == 0
    )
)

print(f"Grupos duplicados: {df_duplicates.count()}")
print(f"Registros con capacidad a revisar: {df_capacity_issues.count()}")

# COMMAND ----------

from pyspark.sql.functions import col, count, when, sum as spark_sum

display(
    df_capacity_issues
    .withColumn(
        "motivo_revision",
        when(
            col("free_bikes").isNull()
            | col("empty_slots").isNull(),
            "Valores nulos"
        ).otherwise("Capacidad total igual a cero")
    )
    .groupBy("motivo_revision")
    .agg(count("*").alias("registros"))
)

# COMMAND ----------

#SOBRE QUE ESTACIONES:


display(
    df_capacity_issues
    .filter(
        (col("free_bikes") + col("empty_slots")) == 0
    )
    .groupBy("id", "name")
    .agg(
        count("*").alias("registros_cero")
    )
    .orderBy(col("registros_cero").desc())
    .limit(20)
)

# COMMAND ----------

from pyspark.sql.functions import countDistinct

display(
    df_silver
    .filter(
        (col("free_bikes") == 0)
        & (col("empty_slots") == 0)
    )
    .groupBy("timestamp")
    .agg(
        count("*").alias("registros_cero"),
        countDistinct("id").alias("estaciones_afectadas")
    )
    .orderBy(col("estaciones_afectadas").desc())
    .limit(10)
)

# COMMAND ----------

from pyspark.sql.functions import col, count, min as spark_min, max as spark_max

display(
    df_silver
    .filter(
        (col("free_bikes") == 0)
        & (col("empty_slots") == 0)
    )
    .groupBy("id", "name")
    .agg(
        count("*").alias("registros_cero"),
        spark_min("timestamp").alias("primer_registro"),
        spark_max("timestamp").alias("ultimo_registro")
    )
    .orderBy(col("registros_cero").desc())
    .limit(10)
)

# COMMAND ----------

display(
    df_silver
    .filter(
        col("id") == "2152f63f9a5f612ce99c8ca42a577251"
    )
    .select(
        "timestamp",
        "free_bikes",
        "empty_slots",
        "slots"
    )
    .orderBy("timestamp")
    .limit(30)
)

# COMMAND ----------

display(
    df_silver
    .filter(
        (col("id") == "2152f63f9a5f612ce99c8ca42a577251")
        & (col("free_bikes") == 0)
        & (col("empty_slots") == 0)
    )
    .select(
        "timestamp",
        "free_bikes",
        "empty_slots",
        "slots",
        "renting",
        "returning",
        "virtual"
    )
    .orderBy("timestamp")
    .limit(20)
)

# COMMAND ----------

from pyspark.sql.functions import count

display(
    df_silver
    .filter(
        (col("free_bikes") == 0)
        & (col("empty_slots") == 0)
    )
    .groupBy("renting", "returning", "virtual", "slots")
    .agg(count("*").alias("registros"))
    .orderBy(col("registros").desc())
)

# COMMAND ----------

# MAGIC %md
# MAGIC Conclusiones de calidad de datos
# MAGIC
# MAGIC Se implementaron controles de integridad para identificar registros sin identificador o timestamp, valores negativos en las métricas de disponibilidad, coordenadas fuera de rango y duplicados por estación y timestamp.
# MAGIC
# MAGIC En la ejecución analizada, no se detectaron registros que incumplieran las reglas iniciales ni duplicados según la clave evaluada.
# MAGIC
# MAGIC También se identificó un patrón recurrente de registros con cero bicicletas y cero espacios disponibles, incluso en estaciones con capacidad declarada. Este comportamiento se documentó como un caso pendiente de interpretación, sin clasificarlo automáticamente como error ni excluirlo del conjunto de datos.
# MAGIC
# MAGIC La cuarentena queda reservada para registros que incumplan reglas de calidad justificadas y verificables.

# COMMAND ----------

tables = [
    "micromobility.streaming.bronze_events",
    "micromobility.streaming.silver_stations",
    "micromobility.streaming.gold_station_behavior",
]

for table_name in tables:
    df = spark.table(table_name)
    print(f"{table_name}: {df.count():,} registros")

# COMMAND ----------

df_silver = spark.table(
    "micromobility.streaming.silver_stations"
)

df_gold = spark.table(
    "micromobility.streaming.gold_station_behavior"
)

print("Silver:", df_silver.count())
print("Gold:", df_gold.count())

print(
    "Estaciones en Silver:",
    df_silver.select("id").distinct().count()
)

print(
    "Estaciones en Gold:",
    df_gold.select("id").distinct().count()
)

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import col, lag

window_station = (
    Window
    .partitionBy("id")
    .orderBy("timestamp")
)

df_check = (
    df_silver
    .withColumn(
        "previous_free_bikes",
        lag("free_bikes").over(window_station)
    )
    .withColumn(
        "previous_timestamp",
        lag("timestamp").over(window_station)
    )
)

print(
    "Sin snapshot anterior:",
    df_check.filter(
        col("previous_free_bikes").isNull()
    ).count()
)

print(
    "Sin intervalo válido:",
    df_check.filter(
        col("previous_free_bikes").isNotNull()
        & (
            col("timestamp").cast("long")
            <= col("previous_timestamp").cast("long")
        )
    ).count()
)

# COMMAND ----------

from pyspark.sql.functions import max

for table_name in [
    "micromobility.streaming.silver_stations",
    "micromobility.streaming.gold_station_behavior"
]:
    df = spark.table(table_name)

    print(
        table_name,
        df.select(max("timestamp").alias("ultimo_timestamp")).first()[0]
    )

# COMMAND ----------

df_silver = spark.table(
    "micromobility.streaming.silver_stations"
)

df_gold = spark.table(
    "micromobility.streaming.gold_station_behavior"
)

print("Silver:", df_silver.count())
print("Gold:", df_gold.count())

# COMMAND ----------

df_check.filter(
    col("previous_free_bikes").isNotNull()
    & (col("previous_timestamp").isNotNull())
    & (
        col("timestamp").cast("long")
        > col("previous_timestamp").cast("long")
    )
).count()

# COMMAND ----------

from pyspark.sql.functions import col, unix_timestamp, round

df_check = df_check.withColumn(
    "minutes_between_snapshots",
    round(
        (
            unix_timestamp("timestamp")
            - unix_timestamp("previous_timestamp")
        ) / 60,
        2
    )
)

print(
    "Con intervalo positivo:",
    df_check.filter(
        col("previous_free_bikes").isNotNull()
        & (col("minutes_between_snapshots") > 0)
    ).count()
)

print("Gold actual:", df_gold.count())

# COMMAND ----------

print("Notebook de validación")
print("Silver:", spark.table(
    "micromobility.streaming.silver_stations"
).count())

print("Gold:", spark.table(
    "micromobility.streaming.gold_station_behavior"
).count())