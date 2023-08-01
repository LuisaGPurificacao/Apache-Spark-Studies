from pyspark.sql import *

from lib.logger import Log4J

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("Hello Spark") \
        .master("local[3]") \
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

    logger.info("Finished HelloSpark")

    spark.stop()
