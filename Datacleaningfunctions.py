# Databricks notebook source
# MAGIC %md
# MAGIC ####### Clean column names

# COMMAND ----------

from pyspark.sql import DataFrame

def clean_column_names(df: DataFrame) -> DataFrame:
    """
    Removes unwanted characters (',', '[', ']', '(', ')', '-', and spaces) from column names of a PySpark DataFrame.
    
    :param df: Input PySpark DataFrame
    :return: DataFrame with cleaned column names
    """
    
    cleaned_columns = [
        col_name.replace("'", "")  
        .replace("[", "")          
        .replace("]", "")          
        .replace(" ", "_")         
        .replace("(", "")          
        .replace(")", "")          
        .replace("-", "")          
        for col_name in df.columns
    ]
    
    for old_name, new_name in zip(df.columns, cleaned_columns):
        df = df.withColumnRenamed(old_name, new_name)
    
    return df




# COMMAND ----------

# MAGIC %md
# MAGIC ####### Column name uniqueness

# COMMAND ----------


from pyspark.sql import DataFrame

def are_column_names_unique(df: DataFrame) -> bool:
    """
    Checks if all column names in a PySpark DataFrame are unique.
    
    :param df: Input PySpark DataFrame
    :return: True if all column names are unique, False otherwise
    """
    
    column_names = df.columns
    
    if len(column_names) == len(set(column_names)):
        print("All column names are unique.")
        return True
    else:
        print("There are duplicate column names.")
        return False


# COMMAND ----------

# MAGIC %md
# MAGIC ####### One-hot enconding

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col, trim, lower
from pyspark.sql.types import ArrayType, StringType

def one_hot_encode(spark_df, column_name):
    """
    Converts an array column into one-hot encoded columns in a PySpark DataFrame.

    Parameters:
    spark_df (DataFrame): Input PySpark DataFrame
    column_name (str): The name of the column containing lists to be one-hot encoded

    Returns:
    DataFrame: Transformed PySpark DataFrame with one-hot encoded columns
    """
    # Ensure the column is an array type (if stored as a string)
    if spark_df.schema[column_name].dataType != ArrayType(StringType()):
        spark_df = spark_df.withColumn(column_name, col(column_name).cast(ArrayType(StringType())))

    
    spark_df = spark_df.withColumn(column_name, 
                                   explode(col(column_name))) 

    spark_df = spark_df.withColumn(column_name, lower(trim(col(column_name))))  


    df_one_hot = spark_df.groupBy("Disease").pivot(column_name).count().fillna(0)

    return df_one_hot






# COMMAND ----------

# MAGIC %md
# MAGIC ####### Clean arrays in column

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, StringType


def clean_column_array(df, column_name, chars_to_remove=" '[]"):
    """
    Clean the specified column in the DataFrame where the column values are arrays
    with unwanted characters (spaces, quotes, brackets, etc.).

    Args:
    df (pyspark.sql.DataFrame): The input DataFrame.
    column_name (str): The name of the column to clean.
    chars_to_remove (str): A string of characters to remove from each element in the array (default is spaces, single quotes, and brackets).

    Returns:
    pyspark.sql.DataFrame: The DataFrame with the cleaned column.
    """
    

    def clean_array_elements(input_array, chars_to_remove):
        return [item.strip(chars_to_remove) for item in input_array]

    clean_array_udf = F.udf(lambda col: clean_array_elements(col, chars_to_remove), ArrayType(StringType()))

    
    df_cleaned = df.withColumn(f"cleaned_{column_name}", clean_array_udf(F.col(column_name)))

    return df_cleaned
