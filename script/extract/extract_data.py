from helper.utils import logging_process
from helper.utils import init_spark_session
import logging
import pyspark

logging_process()


def extract_data(
    data_name: str, format_data: str
) -> pyspark.sql.DataFrame:
    """
    Function to extract movie data in csv or database table

    Parameters
    ----------
    data_name (str): name of data or table of data sources
    format_data (str): format data of data sources, currently on csv or db

    Returns
    -------
    df (pyspark.sql.DataFrame): dataframe of data sources
    """
    # create spark session
    spark = init_spark_session()

    # set variable for database
    DB_URL = "jdbc:postgresql://source_db:5432/source"
    DB_USER = "postgres"
    DB_PASS = "postgres"

    # set config
    jdbc_url = DB_URL
    connection_properties = {
        "user": DB_USER,
        "password": DB_PASS,
        "driver": "org.postgresql.Driver" # set driver postgres
    }

    try:
        if format_data.lower() == "csv":
            logging.info(f"===== Start Extracting {data_name} data =====")

            df = spark.read.csv(f"data/{data_name}.csv", header=True)

            logging.info(f"===== Finish Extracting {data_name} data =====")

            return df

        elif format_data.lower() == "db":
            logging.info(f"===== Start Extracting {data_name} data =====")

            df = spark.read.jdbc(
                url=jdbc_url, table=data_name, properties=connection_properties
            )

            logging.info(f"===== Finish Extracting {data_name} data =====")

            return df

        else:
            raise Exception("Format data not supported yet")

    except Exception as e:
        logging.error("====== Failed to Extract Data ======")
        logging.error(e)

        raise Exception(e)