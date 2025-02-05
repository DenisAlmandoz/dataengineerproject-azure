# Databricks notebook source
spark.sql("""
CREATE CATALOG healthcare
COMMENT 'Catalog for healthcare-related data'
""")


# COMMAND ----------

spark.sql("""
CREATE SCHEMA healthcare.bronze
COMMENT 'Bronze schema for raw data';
""")

spark.sql("""
CREATE SCHEMA healthcare.silver
COMMENT 'Silver schema for cleaned data';
""")

spark.sql("""
CREATE SCHEMA healthcare.gold
COMMENT 'Gold schema for aggregated and analytics-ready data';
""")


# COMMAND ----------

spark.sql("""
CREATE EXTERNAL LOCATION bronze_location
URL 'abfss://bronze@gordeleku.dfs.core.windows.net/'
WITH (STORAGE CREDENTIAL gordecred)
COMMENT 'Bronze layer data in ADLS';
""")

spark.sql("""
CREATE EXTERNAL LOCATION silver_location
URL 'abfss://silver@gordeleku.dfs.core.windows.net/'
WITH (STORAGE CREDENTIAL gordecred)
COMMENT 'Silver layer data in ADLS';
""")

spark.sql("""
CREATE EXTERNAL LOCATION gold_location
URL 'abfss://gold@gordeleku.dfs.core.windows.net/'
WITH (STORAGE CREDENTIAL gordecred)
COMMENT 'Gold layer data in ADLS';
""")

# COMMAND ----------

spark.sql("""
CREATE TABLE healthcare.bronze.description
USING DELTA
LOCATION 'abfss://bronze@gordeleku.dfs.core.windows.net/description/';
""")

spark.sql("""
CREATE TABLE healthcare.bronze.diets
USING DELTA
LOCATION 'abfss://bronze@gordeleku.dfs.core.windows.net/diets/';
""")

spark.sql("""
CREATE TABLE healthcare.bronze.medications
USING DELTA
LOCATION 'abfss://bronze@gordeleku.dfs.core.windows.net/medications/';
""")

spark.sql("""
CREATE TABLE healthcare.bronze.precautions
USING DELTA
LOCATION 'abfss://bronze@gordeleku.dfs.core.windows.net/precautions/';
""")

spark.sql("""
CREATE TABLE healthcare.bronze.symptom_severity
USING DELTA
LOCATION 'abfss://bronze@gordeleku.dfs.core.windows.net/Symptom_severity/';
""")

spark.sql("""
CREATE TABLE healthcare.bronze.symptoms
USING DELTA
LOCATION 'abfss://bronze@gordeleku.dfs.core.windows.net/symtoms/';
""")

spark.sql("""
CREATE TABLE healthcare.bronze.training
USING DELTA
LOCATION 'abfss://bronze@gordeleku.dfs.core.windows.net/Training/';
""")

spark.sql("""
CREATE TABLE healthcare.bronze.workout
USING DELTA
LOCATION 'abfss://bronze@gordeleku.dfs.core.windows.net/workout/';
""")
