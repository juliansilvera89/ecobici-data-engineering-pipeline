# Databricks notebook source
raw_path = "/Volumes/micromobility/streaming/raw_landing/"
schema_path = "/Volumes/micromobility/streaming/raw_landing/_schema"
bronze_checkpoint = "/Volumes/micromobility/streaming/raw_landing/_bronze_checkpoint"

df_stream = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", schema_path)
    .load(raw_path)
)

print("Auto Loader configurado correctamente.")

# COMMAND ----------

query = (
    df_stream.writeStream
    .format("delta")
    .option("checkpointLocation", bronze_checkpoint)
    .outputMode("append")
    .trigger(availableNow=True)
    .toTable("micromobility.streaming.bronze_events")
)

query.awaitTermination()

print("Bronze actualizado correctamente.")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM micromobility.streaming.bronze_events
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS total_registros
# MAGIC FROM micromobility.streaming.bronze_events;