# Databricks notebook source
# COMMAND ----------

kafka_bootstrap = os.getenv("KAFKA_BOOTSTRAP")
kafka_topic = os.getenv("KAFKA_TOPIC")
kafka_api_key = os.getenv("KAFKA_API_KEY")
kafka_api_secret = os.getenv("KAFKA_API_SECRET")

kafka_options = {
    "kafka.bootstrap.servers": kafka_bootstrap,
    "subscribe": kafka_topic,
    "startingOffsets": "earliest",
    "kafka.security.protocol": "SASL_SSL",
    "kafka.sasl.mechanism": "PLAIN",
    "kafka.sasl.jaas.config": f'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username="{kafka_api_key}" password="{kafka_api_secret}";'
}


# COMMAND ----------

df_raw = (
    spark.readStream
    .format("kafka")
    .options(**kafka_options)
    .load()
)

df_raw.printSchema()


# COMMAND ----------

# Read raw stream from Kafka
df_raw = (
    spark.readStream
    .format("kafka")
    .options(**kafka_options)
    .load()
)

# Show schema to validate payload structure
df_raw.printSchema()


# COMMAND ----------

# COMMAND ----------

from pyspark.sql.types import StructType, StringType, TimestampType
from pyspark.sql.functions import from_json, col

# Define expected schema of Kafka messages
event_schema = StructType() \
    .add("event_id", StringType()) \
    .add("event_type", StringType()) \
    .add("timestamp", TimestampType()) \
    .add("value", StringType())

# Parse JSON from Kafka 'value' column
df_parsed = df_raw.select(
    from_json(col("value").cast("string"), event_schema).alias("data")
).select("data.*")

# Preview parsed schema
df_parsed.printSchema()


# COMMAND ----------

# Stream parsed Kafka JSON into Bronze Delta table
(
    df_parsed.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", "/tmp/kafka_checkpoint_bronze")  # Use DBFS path in prod
    .table("bronze_events")
)


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM bronze_events
# MAGIC ORDER BY timestamp DESC
# MAGIC LIMIT 20
# MAGIC
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

df_parsed.filter(col("value").isNotNull()).writeStream \
    .format("console") \
    .option("truncate", False) \
    .start()


# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM bronze_events ORDER BY timestamp DESC LIMIT 10;
# MAGIC
# MAGIC

# COMMAND ----------

for stream in spark.streams.active:
    print(f"Stopping stream: {stream.name}")
    stream.stop()
