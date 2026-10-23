# Databricks notebook source
df_bronze = spark.table(
    "micromobility.streaming.bronze_events"
)

display(df_bronze.limit(10))

# COMMAND ----------

df_bronze.printSchema()

# COMMAND ----------

from pyspark.sql.functions import col, regexp_replace, to_timestamp

df_silver = (
    df_bronze
    .withColumn("free_bikes", col("free_bikes").cast("int"))
    .withColumn("empty_slots", col("empty_slots").cast("int"))
    .withColumn("latitude", col("latitude").cast("double"))
    .withColumn("longitude", col("longitude").cast("double"))
    .withColumn(
        "timestamp",
        to_timestamp(
            regexp_replace(col("timestamp"), r"\+00:00Z$", "Z")
        )
    )
)

display(df_silver.limit(10))

# COMMAND ----------

display(
    df_silver.select(
        "id",
        "name",
        "free_bikes",
        "empty_slots",
        "latitude",
        "longitude",
        "timestamp"
    ).summary()
)

# COMMAND ----------

from pyspark.sql.functions import col, sum

display(
    df_silver.select(
        [
            sum(col(c).isNull().cast("int")).alias(c)
            for c in [
                "id",
                "name",
                "free_bikes",
                "empty_slots",
                "latitude",
                "longitude",
                "timestamp"
            ]
        ]
    )
)

# COMMAND ----------

display(
    df_silver.filter(
        (col("free_bikes") < 0) |
        (col("empty_slots") < 0) |
        (col("latitude") < -90) |
        (col("latitude") > 90) |
        (col("longitude") < -180) |
        (col("longitude") > 180)
    )
)

# COMMAND ----------

display(
    df_silver.select("extra").limit(5)
)

# COMMAND ----------

from pyspark.sql.functions import from_json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType

extra_schema = StructType([
    StructField("uid", StringType(), True),
    StructField("renting", IntegerType(), True),
    StructField("returning", IntegerType(), True),
    StructField("last_updated", StringType(), True),
    StructField("payment", StringType(), True),
    StructField("payment-terminal", BooleanType(), True),
    StructField("slots", IntegerType(), True),
    StructField("virtual", BooleanType(), True)
])

df_silver = df_silver.withColumn(
    "extra_parsed",
    from_json(col("extra"), extra_schema)
)

display(df_silver.select("extra_parsed").limit(5))

# COMMAND ----------

df_silver = (
    df_silver
    .withColumn("station_uid", col("extra_parsed.uid"))
    .withColumn("renting", col("extra_parsed.renting"))
    .withColumn("returning", col("extra_parsed.returning"))
    .withColumn("slots", col("extra_parsed.slots"))
    .withColumn("virtual", col("extra_parsed.virtual"))
)

display(
    df_silver.select(
        "id",
        "name",
        "free_bikes",
        "empty_slots",
        "station_uid",
        "renting",
        "returning",
        "slots",
        "virtual"
    ).limit(10)
)

# COMMAND ----------

df_silver = df_silver.drop(
    "extra",
    "extra_parsed"
)

display(df_silver.limit(5))

# COMMAND ----------

df_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("micromobility.streaming.silver_stations")

print("Tabla Silver creada correctamente.")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     COUNT(*) AS total_registros,
# MAGIC     COUNT(DISTINCT id) AS estaciones_unicas
# MAGIC FROM micromobility.streaming.silver_stations;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM micromobility.streaming.silver_stations
# MAGIC limit (5);