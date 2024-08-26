from pyspark.sql import SparkSession
from pyspark.sql import functions as func

spark = SparkSession.builder.appName("FriendsByAge").getOrCreate()

people = spark.read.option("header", "true").option("inferSchema", "true")\
  .csv("file:///Users/steam/repos/spark-course/fakefriends-header.csv")

people.select("age", "friends").groupBy("age").agg(func.round(func.avg("friends"), 2).alias("avgFriends")).sort("age").show()

spark.stop()