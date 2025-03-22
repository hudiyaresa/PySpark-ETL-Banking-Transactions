from pyspark.sql import DataFrame

def rename_columns(df: DataFrame, table_name: str) -> DataFrame:
    """
    Function to rename columns based on the table name.
    """
    rename_mappings = {
        "marketing_campaign_deposit": {
            "pdays": "days_since_last_campaign",
            "previous": "previous_campaign_contacts",
            "poutcome": "previous_campaign_outcome"
        },
        "customers": {
            "CustomerID": "customer_id",
            "CustomerDOB": "birth_date",
            "CustGender": "gender",
            "CustLocation": "location",
            "CustAccountBalance": "account_balance"
        },
        "transactions": {
            "TransactionID": "transaction_id",
            "CustomerID": "customer_id",
            "TransactionDate": "transaction_date",
            "TransactionTime": "transaction_time",
            "TransactionAmount (INR)": "transaction_amount"
        }
    }
    
    if table_name in rename_mappings:
        for old_col, new_col in rename_mappings[table_name].items():
            df = df.withColumnRenamed(old_col, new_col)
    
    return df
