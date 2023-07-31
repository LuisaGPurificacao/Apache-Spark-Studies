from pyspark.sql import *

if __name__ == "__main__":

    spark = SparkSession.builder \
            .appName("Hello Spark") \
            .master("local[2]") \
            .getOrCreate()

    data_list = [
        ("Luisa", 18),
        ("Lívia", 19),
        ("Marcela", 18)
    ]

    df = spark.createDataFrame(data_list).toDF("Name", "Age")

    df.show()