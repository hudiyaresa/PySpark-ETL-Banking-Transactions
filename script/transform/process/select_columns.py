from helper.utils import logging_process
import logging
import pyspark

logging_process()


def select_columns_process(
    df_result: pyspark.sql.DataFrame, table_name: str
) -> pyspark.sql.DataFrame:
    """
    Function that selects columns based on the table name from the list of columns.

    Parameters
    ----------
    df_result (pyspark.sql.DataFrame): Input DataFrame for the specific table.
    table_name (str): The name of the table used to select the appropriate columns.

    Returns
    -------
    pyspark.sql.DataFrame: DataFrame with selected columns.
    """
    try:
        logging.info(f"===== Start Selecting Data process for table {table_name} =====")

        # Define columns for each table
        table_columns = {
            "marital_status": ["marital_id", "value", "created_at", "updated_at"],
            "education_status": ["education_id", "value", "created_at", "updated_at"],
            "marketing_campaign_deposit": [
                "loan_data_id", "age", "job", "marital_id", "education_id", "default", "balance",
                "housing", "loan", "contact", "day", "month", "duration", "duration_in_year", 
                "campaign", "days_since_last_campaign", "previous_campaign_contacts", 
                "previous_campaign_outcome", "subscribed_deposit", "created_at", "updated_at"
            ],
            "customers": [
                "customer_id", "birth_date", "gender", "location", "account_balance", "created_at", "updated_at"
            ],
            "transactions": [
                "transaction_id", "customer_id", "transaction_date", "transaction_time", 
                "transaction_amount", "created_at", "updated_at"
            ]
        }

        # Check if the table_name is in the dictionary and select the columns
        if table_name in table_columns:
            selected_cols = table_columns[table_name]
            df_result = df_result.select(*selected_cols)
        else:
            raise ValueError(f"Table name '{table_name}' not recognized!")

        logging.info(f"===== Finish Selecting Data process for table {table_name} =====")

        return df_result

    except Exception as e:
        logging.error(f"===== Failed Selecting Data process for table {table_name} =====")
        logging.error(e)
        raise Exception(e)
