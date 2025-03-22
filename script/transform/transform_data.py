import logging
from pyspark.sql import DataFrame
from helper.utils import logging_process
from transform.process.rename_columns import rename_columns
from transform.process.convert_date import convert_date_columns
from transform.process.casting_data import casting_data_types
from transform.process.select_columns import select_columns_process
from transform.process.clean_data import clean_data

logging_process()

def transform_data(df: DataFrame, table_name: str) -> DataFrame:
    """
    Function to apply all transformation steps on the dataframe.
    """
    try:
        logging.info(f"===== Start Transforming Data for {table_name} =====")
        df = rename_columns(df, table_name)
        df = convert_date_columns(df, table_name)
        df = casting_data_types(df, table_name)
        df = select_columns_process(df, table_name)
        df = clean_data(df, table_name)
        logging.info(f"===== Finished Transforming Data for {table_name} =====")
        return df
    except Exception as e:
        logging.error(f"===== Failed to Transform Data for {table_name} =====")
        logging.error(e)
        raise
