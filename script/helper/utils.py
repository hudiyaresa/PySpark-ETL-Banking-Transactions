import logging
import os
from pyspark.sql import SparkSession


def logging_process(log_file="script/log/info.log"):
    # Configure logging
    os.makedirs(os.path.dirname(log_file), exist_ok=True)
    logging.basicConfig(
        filename=log_file,
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s"
    )
    logger = logging.getLogger()
    return logger


def init_spark_session():
    spark = SparkSession.builder.appName(
        "Exercise Data Pipeline Week_6"
    ).getOrCreate()

    # handle legacy time parser
    spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

    return spark