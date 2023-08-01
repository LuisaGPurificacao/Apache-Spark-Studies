import sys

from pyspark.sql import *

from lib.logger import Log4J
from lib.utils import get_spark_app_config
from lib.utils import load_survey_df

if __name__ == "__main__":
    conf = get_spark_app_config()
    spark = SparkSession.builder \
        .config(conf=conf) \
        .getOrCreate()

    logger = Log4J(spark)

    if len(sys.argv) != 2:
        logger.error("Usage: HelloSpark <filename>")
        sys.exit(-1)

    logger.info("Starting HelloSpark")

    survey_df = load_survey_df(spark, sys.argv[1])

    survey_df.show()

    logger.info("Finished HelloSpark")

    # spark.stop()
