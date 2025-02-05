# Databricks notebook source

spark.sql("SHOW TABLES IN healthcare.silver").show()




# COMMAND ----------


spark.sql("DESCRIBE EXTENDED healthcare.silver.diet").show()

# COMMAND ----------


DESCRIBE HISTORY healthcare.silver.diet

# COMMAND ----------

# MAGIC %md
# MAGIC # AGGREGATED DATA
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM healthcare.silver.diet

# COMMAND ----------

# MAGIC %sql
# MAGIC GRANT SELECT ON TABLE healthcare.silver.diet TO `Data scientist group`;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM healthcare.silver.training

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT Disease, COUNT(*)as total_count FROM healthcare.silver.training
# MAGIC GROUP BY Disease
# MAGIC

# COMMAND ----------

spark.sql("USE healthcare.silver") 


# COMMAND ----------

df = spark.table("training")

# List all columns except 'column_to_exclude'
columns_to_select = [col for col in df.columns if col != 'Disease']

# Select all columns except 'column_to_exclude'
df1= df[columns_to_select]

# COMMAND ----------

from pyspark.sql.functions import col

for column in df1.columns:
    df1 = df1.withColumn(column, col(column).cast("int"))  # Cast each column to IntegerType

# Show the result with integer columns
df1.printSchema()

# COMMAND ----------

from pyspark.sql import functions as F

df_columns = [col for col in df.columns if col != 'Disease']

df1_columns = df1.columns  

df_joined = df.join(df1, how="inner")

df_final_training = df_joined.select(df.Disease, *[df1[col] for col in df1_columns])

df_final_training.printSchema()

# COMMAND ----------

from pyspark.sql import functions as F

columns_to_sum = [col for col in df_final_training.columns if col != 'Disease']

df_grouped = df.groupBy('Disease').agg(
    *[F.sum(col).alias(col) for col in columns_to_sum]
)
display(df_grouped)


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM healthcare.silver.medication

# COMMAND ----------

# MAGIC %sql
# MAGIC GRANT SELECT ON TABLE healthcare.silver.medication TO `Data scientist group`;

# COMMAND ----------

# MAGIC %md
# MAGIC # CREATING GOLD DELTA TABLES
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS healthcare.gold.training
# MAGIC USING DELTA
# MAGIC

# COMMAND ----------

df_grouped.write.mode("overwrite").option("mergeSchema", "true").saveAsTable("healthcare.gold.training")

# COMMAND ----------

# MAGIC %md
# MAGIC # ASSIGNING PERMISSIONS

# COMMAND ----------

# MAGIC %sql
# MAGIC GRANT ALL PRIVILEGES ON TABLE healthcare.gold.training 
# MAGIC TO `Data scientist group`;