import logging
from extract.extract_data import extract_data
from transform.transform_data import transform_data
from load.load_data import load_data
from helper.utils import logging_process

# Initialize logging
logging_process()

if __name__ == "__main__":
    logging.info("===== Start Banking Data Pipeline =====")

    try:
        # Extract data from CSV and database
        df_transactions = extract_data(data_name="new_bank_transaction", format_data="csv")
        df_customers = extract_data(data_name="new_bank_transaction", format_data="csv")
        df_marketing = extract_data(data_name="marketing_campaign_deposit", format_data="db")
        df_education = extract_data(data_name="education_status", format_data="db")
        df_marital = extract_data(data_name="marital_status", format_data="db")

        # Transform each dataset separately
        df_transactions = transform_data(df_transactions, "transactions")
        df_customers = transform_data(df_customers, "customers")
        df_marketing = transform_data(df_marketing, "marketing_campaign_deposit")
        df_education = transform_data(df_education, "education_status")
        df_marital = transform_data(df_marital, "marital_status")

        # Load each transformed dataset into the data warehouse
        load_data(df_transactions, table_name="transactions")
        load_data(df_customers, table_name="customers")
        load_data(df_marketing, table_name="marketing_campaign_deposit")
        load_data(df_education, table_name="education_status")
        load_data(df_marital, table_name="marital_status")

        logging.info("===== Finish Banking Data Pipeline =====")

    except Exception as e:
        logging.error("===== Data Pipeline Failed =====")
        logging.error(e)
        raise
