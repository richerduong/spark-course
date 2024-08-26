from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

spark = SparkSession.builder.appName("TotalSpentByCustomer").getOrCreate()

schema = StructType([ \
  StructField("customerID", IntegerType(), True), \
  StructField("itemID", IntegerType(), True), \
  StructField("price", FloatType(), True) \
])

df = spark.read.schema(schema).csv("file:///Users/steam/repos/spark-course/customer-orders.csv")

customerAndPrice = df.select("customerID", "price")
customerAndPrice.groupBy("customerID").agg(func.round(func.sum("price"), 2).alias("amountSpent")).sort("amountSpent").show(customerAndPrice.count())

spark.stop()