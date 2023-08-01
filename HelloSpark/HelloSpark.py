from pyspark.sql import *

from lib.logger import Log4J
from lib.utils import get_spark_app_config

if __name__ == "__main__":
    conf = get_spark_app_config()
    spark = SparkSession.builder \
        .config(conf=conf) \
        .getOrCreate()

    logger = Log4J(spark)

    logger.info("Starting HelloSpark")

    data_list = [
        ("Luisa", 18),
        ("Lívia", 19),
        ("Marcela", 18)
    ]

    logger.info("Creating dataframe")
    df = spark.createDataFrame(data_list).toDF("Name", "Age")
    logger.info("Dataframe created")

    df.show()

    conf_out = spark.sparkContext.getConf()
    logger.info(conf_out.toDebugString())

    logger.info("Finished HelloSpark")

    spark.stop()
