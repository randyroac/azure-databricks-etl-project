# Databricks notebook source
# MAGIC %md
# MAGIC #### Ingest drivers.json file

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 1 - Read the JSON file using the spark dataframe reader API

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType

# COMMAND ----------

name_schema = StructType(fields=[StructField("forename", StringType(),True),
                                 StructField("surname", StringType(),True)

                                
                                
                                
                                
                                ])

# COMMAND ----------

drivers_schema = StructType(fields=[StructField("driverId", IntegerType(),False),
                                    StructField("driverRef", StringType(), True),
                                    StructField("number", IntegerType(),True),
                                    StructField("code", StringType(),True),
                                    StructField("name", name_schema),
                                    StructField("dob", DateType(),True),
                                    StructField("nationality", StringType(), True),
                                    StructField("url", StringType(), True)
                                    
                                   
                                   
                                   
                                   
                                   
                                   ])

# COMMAND ----------

drivers_df =spark.read\
.schema(drivers_schema) \
.json("/mnt/motorraste1/raw/drivers.json")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 - Rename columns and add new columns
# MAGIC 1. driverId renamed to driver_id
# MAGIC 2. driverRef renamed to driver_ref
# MAGIC 3. ingestion date added
# MAGIC 4. name added with condatenation of forename and surname

# COMMAND ----------

from pyspark.sql.functions import col, concat, current_timestamp, lit

# COMMAND ----------

drivers_with_columns_df = drivers_df.withColumnRenamed("driverId", "driver_id")\
                                    .withColumnRenamed("driverRef", "driver_ref")\
                                    .withColumn("ingestion_date", current_timestamp())\
                                    .withColumn("name",concat(col("name.forename"), lit(" "), col("name.surname")))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step3 - Drop the unwanted columns
# MAGIC 1. name.forename
# MAGIC 2. name.surname
# MAGIC 3. url

# COMMAND ----------

drivers_final_df = drivers_with_columns_df.drop(col("url"))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 4 - Write output to parquet file

# COMMAND ----------

drivers_final_df.write.mode("overwrite").parquet("/mnt/motorraste1/processed/drivers")
