from pyspark.sql import DataFrame
from pyspark.sql.functions import col, trim, lower, when

def clean_data(df: DataFrame, table_name: str) -> DataFrame:
    """
    Function to clean data before processing.
    """
    if table_name == "customers":
        df = df.withColumn("customer_gender", lower(trim(col("customer_gender"))))
        df = df.withColumn("customer_gender", when(col("customer_gender") == "m", "Male")
                           .when(col("customer_gender") == "f", "Female")
                           .otherwise("Other"))
       
    return df
