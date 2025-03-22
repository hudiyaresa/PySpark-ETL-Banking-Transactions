from pyspark.sql import DataFrame
from pyspark.sql.functions import to_date, col, when, concat, substring, lit, regexp_extract

def convert_date_columns(df: DataFrame, table_name: str) -> DataFrame:
    """
    Function to convert date and time columns based on table name.
    
    Args:
        df: Input DataFrame
        table_name: Name of the table being processed
        
    Returns:
        DataFrame with converted date and time columns
    """
    if table_name == "transactions":
        # Convert TransactionDate from d/M/yy to YYYY/MM/DD
        df = df.withColumn("transaction_date", to_date(col("TransactionDate"), "d/M/yy"))
        
        # Convert TransactionTime from HHMMSS to HH:MM:SS format using safe pattern matching
        df = df.withColumn(
            "transaction_time",
            concat(
                regexp_extract(col("TransactionTime"), "^(\\d{2})", 1),
                lit(":"),
                regexp_extract(col("TransactionTime"), "^\\d{2}(\\d{2})", 1),
                lit(":"),
                regexp_extract(col("TransactionTime"), "^\\d{4}(\\d{2})", 1)
            )
        )
        
        # Also handle CustomerDOB in transactions table if it exists
        if "CustomerDOB" in df.columns:
            df = df.withColumn(
                "customer_birth_date",
                when(
                    regexp_extract(col("CustomerDOB"), "(\\d{1,2})/(\\d{1,2})/(\\d{2})", 3).cast("int") > 25,
                    to_date(
                        concat(
                            regexp_extract(col("CustomerDOB"), "(\\d{1,2})/(\\d{1,2})/", 1), 
                            lit("/"),
                            regexp_extract(col("CustomerDOB"), "\\d{1,2}/(\\d{1,2})/", 1),
                            lit("/19"),
                            regexp_extract(col("CustomerDOB"), "\\d{1,2}/\\d{1,2}/(\\d{2})", 1)
                        ),
                        "d/M/yyyy"
                    )
                ).otherwise(to_date(col("CustomerDOB"), "d/M/yy"))
            )

    elif table_name == "customers":
        # Convert CustomerDOB with year > 25 check using regex for safer extraction
        df = df.withColumn(
            "birth_date",
            when(
                regexp_extract(col("CustomerDOB"), "(\\d{1,2})/(\\d{1,2})/(\\d{2})", 3).cast("int") > 25,
                to_date(
                    concat(
                        regexp_extract(col("CustomerDOB"), "(\\d{1,2})/(\\d{1,2})/", 1), 
                        lit("/"),
                        regexp_extract(col("CustomerDOB"), "\\d{1,2}/(\\d{1,2})/", 1),
                        lit("/19"),
                        regexp_extract(col("CustomerDOB"), "\\d{1,2}/\\d{1,2}/(\\d{2})", 1)
                    ),
                    "d/M/yyyy"
                )
            ).otherwise(to_date(col("CustomerDOB"), "d/M/yy"))
        )
        
    return df