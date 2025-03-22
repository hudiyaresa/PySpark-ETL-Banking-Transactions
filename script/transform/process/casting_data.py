from pyspark.sql import DataFrame
from pyspark.sql.functions import col, round, regexp_replace

def casting_data_types(df: DataFrame, table_name: str) -> DataFrame:
    """
    Function to cast data types based on table name.
    """
    casting_mappings = {
        "marketing_campaign_deposit": {
            "balance": ("int", "\\$"),
            "duration_in_year": ("int", None, "duration", 365)
        },
        "transactions": {
            "transaction_amount": "double"
        },
        "customers": {
            "account_balance": "double"
        }
    }
    
    if table_name in casting_mappings:
        for col_name, cast_info in casting_mappings[table_name].items():
            if isinstance(cast_info, tuple):
                if len(cast_info) == 2 and cast_info[1]:
                    df = df.withColumn(col_name, regexp_replace(col(col_name), cast_info[1], "").cast(cast_info[0]))
                elif len(cast_info) == 4:
                    df = df.withColumn(col_name, round(col(cast_info[2]) / cast_info[3]).cast(cast_info[0]))
            else:
                df = df.withColumn(col_name, col(col_name).cast(cast_info))
    
    return df
