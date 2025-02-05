# Databricks notebook source
from PIL import Image
import IPython.display as display

# Use the DBFS path
image_path = "/dbfs/FileStore/tables/diagram/Diagram_drawio.png"

# Open and display the image
img = Image.open(image_path)
display.display(img)



# COMMAND ----------

# MAGIC %md
# MAGIC ## IMPORTING FUNCTIONS

# COMMAND ----------

# MAGIC
# MAGIC %run "./Datacleaningfunctions"
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## READING FROM AZURE STORAGE - BRONZE LAYER - RAW DATA

# COMMAND ----------

bronze_description_df = spark.read.format("csv").option("header", "true").load("abfss://bronze@gordeleku.dfs.core.windows.net/description/description.csv")

bronze_medication_df = spark.read.format("csv").option("header", "true").load("abfss://bronze@gordeleku.dfs.core.windows.net/medications/medications.csv")

bronze_training_df = spark.read.format("csv").option("header", "true").load("abfss://bronze@gordeleku.dfs.core.windows.net/Training/Training.csv")

bronze_diets_df = spark.read.format("csv").option("header", "true").load("abfss://bronze@gordeleku.dfs.core.windows.net/diets/diets.csv")

bronze_precautions_df = spark.read.format("csv").option("header", "true").load("abfss://bronze@gordeleku.dfs.core.windows.net/precautions/precautions.csv")

bronze_Symptom_severity_df = spark.read.format("csv").option("header", "true").load("abfss://bronze@gordeleku.dfs.core.windows.net/Symptom-severity/Symptom-severity.csv")

bronze_symptons_df = spark.read.format("csv").option("header", "true").load("abfss://bronze@gordeleku.dfs.core.windows.net/symtoms/symtoms.csv")

bronze_workout_df = spark.read.format("csv").option("header", "true").load("abfss://bronze@gordeleku.dfs.core.windows.net/workout/workout.csv")






# COMMAND ----------

display(bronze_workout_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #DATA CLEANING

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Silver medication dataframe
# MAGIC

# COMMAND ----------



bronze_medication_df.printSchema()
    



# COMMAND ----------

# MAGIC %md
# MAGIC ##### Converting list of strings to array 

# COMMAND ----------

from pyspark.sql.functions import explode, col, lit, split

bronze_medication_df = bronze_medication_df.withColumn("Medication", split(col("Medication"), ","))




# COMMAND ----------

bronze_medication_df.printSchema()

# COMMAND ----------

display(bronze_medication_df)

# COMMAND ----------

bronze_medication_df1 = clean_column_array(bronze_medication_df, "Medication")

# COMMAND ----------

display(bronze_medication_df1)

# COMMAND ----------

bronze_medication_df2 = bronze_medication_df1.drop("Medication")

# COMMAND ----------

display(bronze_medication_df2)

# COMMAND ----------

# MAGIC %md
# MAGIC ####One-hot-encoding

# COMMAND ----------

bronze_medication_df3 = one_hot_encode(bronze_medication_df2, "cleaned_Medication")

display(bronze_medication_df3)


# COMMAND ----------

are_column_names_unique(bronze_medication_df3)

# COMMAND ----------

bronze_medication_df4= clean_column_names(bronze_medication_df3)

# COMMAND ----------

display(bronze_medication_df4)

# COMMAND ----------

# MAGIC %md
# MAGIC ####Writing data to ADLS

# COMMAND ----------

bronze_medication_df4.write.format("delta").mode("overwrite").save("abfss://silver@gordeleku.dfs.core.windows.net/medication/")


# COMMAND ----------

# MAGIC %md
# MAGIC ###2. Silver diet dataframe

# COMMAND ----------

display(bronze_diets_df)

# COMMAND ----------



from pyspark.sql.functions import explode, col, lit, split

bronze_diets_df = bronze_diets_df.withColumn("Diet", split(col("Diet"), ","))


# COMMAND ----------

bronze_diets_df.printSchema

# COMMAND ----------

bronze_diets_df1 = clean_column_array(bronze_diets_df, "Diet")

# COMMAND ----------

bronze_diets_df2 = bronze_diets_df1.drop("Diet")

# COMMAND ----------

Bronze_diets_df3 = one_hot_encode(bronze_diets_df2,"cleaned_Diet")

# COMMAND ----------

display(Bronze_diets_df3)

# COMMAND ----------

Bronze_diets_df4 = clean_column_names(Bronze_diets_df3)

# COMMAND ----------

are_column_names_unique(Bronze_diets_df4)

# COMMAND ----------

Bronze_diets_df4.write.format("delta").mode("overwrite").save("abfss://silver@gordeleku.dfs.core.windows.net/diet/")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Silver precaution dataframe

# COMMAND ----------

display(bronze_precautions_df)

# COMMAND ----------

# Drop the column "_c0" from the DataFrame
bronze_precautions_df1 = bronze_precautions_df.drop("_c0")




# COMMAND ----------

display(bronze_precautions_df1)

# COMMAND ----------

bronze_precautions_df1.write.format("delta").mode("overwrite").save("abfss://silver@gordeleku.dfs.core.windows.net/precaution/")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Silver symptons dataframe

# COMMAND ----------

display(bronze_symptons_df)

# COMMAND ----------

bronze_symptons_df.drop("_c0").write.format("delta").mode("overwrite").save("abfss://silver@gordeleku.dfs.core.windows.net/symptom/")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## 5. Silver training dataframe

# COMMAND ----------

display(bronze_training_df)

# COMMAND ----------

bronze_training_df1 = bronze_training_df.withColumnRenamed("prognosis", "Disease")

# COMMAND ----------

display(bronze_training_df1)

# COMMAND ----------

bronze_training_df1 = clean_column_names(bronze_training_df1)

# COMMAND ----------

bronze_training_df1.columns

# COMMAND ----------

bronze_training_df1.write.format("delta").option("mergeSchema", "true").mode("overwrite").save("abfss://silver@gordeleku.dfs.core.windows.net/training/")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##6. Silver workout dataframe

# COMMAND ----------

display(bronze_workout_df1)

# COMMAND ----------


bronze_workout_df1 = bronze_workout_df.withColumnRenamed("disease", "Disease") \
                                        .drop("_c0", "Unnamed: 0")


display(bronze_workout_df1)


# COMMAND ----------

bronze_workout_df1.write \
    .format("delta") \
    .partitionBy("Disease") \
    .mode("overwrite") \
    .save("abfss://silver@gordeleku.dfs.core.windows.net/workout")



# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Silver symptons severity dataframe

# COMMAND ----------

display(bronze_Symptom_severity_df)

# COMMAND ----------

bronze_Symptom_severity_df.write.format("delta").mode("overwrite").save("abfss://silver@gordeleku.dfs.core.windows.net/symptom_severity/")

# COMMAND ----------

# MAGIC %md
# MAGIC ##8. Silver description dataframe

# COMMAND ----------

display(bronze_description_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #CREATING METASTORE TABLES

# COMMAND ----------


spark.sql("""
CREATE TABLE IF NOT EXISTS healthcare.gold.medication
USING DELTA
LOCATION 'abfss://gold@gordeleku.dfs.core.windows.net/medication/';
""")


spark.sql("""
CREATE TABLE IF NOT EXISTS healthcare.gold.training
USING DELTA
LOCATION 'abfss://gold@gordeleku.dfs.core.windows.net/training/';
""")


