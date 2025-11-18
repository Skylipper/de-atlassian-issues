import os

from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .master("local") \
    .config("spark.executor.memory", "1g") \
    .config("spark.executor.cores", 1) \
    .config("spark.driver.cores", 2) \
    .config("spark.driver.memory", "1g") \
    .appName("7441") \
    .getOrCreate()

data = [("Max", 55),
        ('Yan', 53),
        ('Dmitry', 54),
        ('Ann', 25)]

print(data)

columns = ['Name', 'Age']
df = spark.createDataFrame(data=data, schema=columns)

df.printSchema()
