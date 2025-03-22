from helper.utils import logging_process
import logging
import pyspark

logging_process()


def load_data(df_result: pyspark.sql.DataFrame, table_name: str) -> None:
    """
    Function that used to dump the result to the database using PySpark
    and maintains data integrity by truncating the table before loading new data.

    Parameters
    ----------
    df_result (pyspark.sql.DataFrame): final result of pyspark movie dataframe
    table_name (str): The target table name in the database where data needs to be loaded.
    """
    try:
        # set variable for database
        DB_URL = "jdbc:postgresql://data_warehouse:5432/data_warehouse"
        DB_USER = "postgres"
        DB_PASS = "postgres"

        # set config
        jdbc_url = DB_URL
        connection_properties = {
            "user": DB_USER,
            "password": DB_PASS,
            "driver": "org.postgresql.Driver" # set driver postgres
        }

        logging.info("===== Start Load data to the database =====")

        # Truncate the target table (ensure data integrity by removing old records before loading new data)
        truncate_sql = f"TRUNCATE TABLE {table_name} CASCADE"

        # Use the jdbc connection to execute the SQL truncate statement
        truncate_df = pyspark.sql.SparkSession.builder.getOrCreate().read \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", f"({truncate_sql}) AS trunc_query") \
            .option("user", DB_USER) \
            .option("password", DB_PASS) \
            .load()

        logging.info(f"===== Truncated table {table_name} successfully =====")                

        # load data
        df_result.write.jdbc(
            url=jdbc_url,
            table=table_name,
            mode="append",   # Use append to add data to the table without deleting existing data
            properties=connection_properties,
        )

        logging.info("===== Finish Load data to the database =====")

    except Exception as e:
        logging.error("===== Failed Load data to the database =====")
        logging.error(e)
        raise Exception(e)