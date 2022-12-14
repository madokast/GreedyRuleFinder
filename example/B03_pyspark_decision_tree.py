from pyspark.sql import SparkSession, DataFrame

spark = SparkSession.builder.appName(__name__).getOrCreate()

data:DataFrame = spark.read.format("csv").option("header", True).load("testdata/relation.csv").cache()

data.show()

all_columns = data.columns

