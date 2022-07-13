# Databricks notebook source
# MAGIC %md
# MAGIC ####Ingest results.json file

# COMMAND ----------

# MAGIC %md
# MAGIC #####Step 1: Read the JSON file using the spark dataframe reader API

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType

# COMMAND ----------

results_schema = StructType(fields=[StructField("resultId", IntegerType(), False),
                                   StructField("raceId", IntegerType(), True),
                                   StructField("driverId", IntegerType(), True),
                                   StructField("constructorId", IntegerType(), True),
                                   StructField("number", IntegerType(), True),
                                   StructField("grid", IntegerType(), True),
                                   StructField("position", IntegerType(), True),
                                   StructField("positionText", StringType(), True),
                                   StructField("positionOrder", IntegerType(), True),
                                   StructField("points", FloatType(), True),
                                   StructField("laps", IntegerType(), True),
                                   StructField("time", StringType(), True),
                                   StructField("millisecond", IntegerType(), True),
                                   StructField("fastestlap", IntegerType(), True),
                                   StructField("rank", IntegerType(), True),
                                   StructField("fastestLapTime", StringType(), True),
                                   StructField("fastestLapSpeed", FloatType(), True),
                                   StructField("statusId", StringType(), True)
                 
                                  ])

# COMMAND ----------

results_schema2 = "resultId INT, raceId INT, driverId INT, constructorId INT, positionText STRING, positionOrder STRING, points FLOAT, fastestLap INT, fastestLapTime STRING, fastestLapSpeed STRING, statusId INT"

# COMMAND ----------

results_df = spark.read\
.schema(results_schema)\
.json("/mnt/motorraste1/raw/results.json")

# COMMAND ----------

results_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 - Rename columns and add new columns
# MAGIC 1. resultId renamed to result_id
# MAGIC 2. raceId renamed to race_id
# MAGIC 3. driverId renamed to driver_id
# MAGIC 4. constructorId renamed to constructor_id
# MAGIC 5. positionText renamed to position_text
# MAGIC 6. positionOrder renamed to position_order
# MAGIC 7. fastestLap renamed to fastest_lap
# MAGIC 8. fastestLapTime renamed to fastest_lap_time
# MAGIC 8. FastestLapSpeed renamed to fastest_lap_speed
# MAGIC 9. add ingestion_date

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, col

# COMMAND ----------

results_with_columns_df = results_df.withColumnRenamed("resultId", "result_id")\
                                    .withColumnRenamed("raceId", "race_id")\
                                    .withColumnRenamed("driverId", "driver_id")\
                                    .withColumnRenamed("constructorId","constructor_id")\
                                    .withColumnRenamed("positionText","position_text")\
                                    .withColumnRenamed("positionOrder","position_order")\
                                    .withColumnRenamed("fastestLap","fastest_lap")\
                                    .withColumnRenamed("fastestLapTime","fastest_lap_time")\
                                    .withColumnRenamed("fastestLapSpeed","fastest_lap_speed")\
                                    .withColumn("ingestion_date",current_timestamp())



# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 - Drop the unwanted columns
# MAGIC 1. statusId

# COMMAND ----------

results_final_df = results_with_columns_df.drop(col("statusId"))

# COMMAND ----------

# MAGIC %md
# MAGIC #####Step 4 - Write to parquet file in processed folder

# COMMAND ----------

results_final_df.write.mode("overwrite").partitionBy("race_id").parquet("/mnt/motorraste1/processed/results")

# COMMAND ----------

results_final_df.printSchema()

# COMMAND ----------

display(results_final_df)

# COMMAND ----------

display(spark.read.parquet("/mnt/motorraste1/processed/results"))
