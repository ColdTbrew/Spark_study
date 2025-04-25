from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
spark = SparkSession.builder.appName("TotalSpentCustomer").getOrCreate()

schema = StructType([
    StructField("customerID", StringType(), True),
    StructField("itemID", StringType(), True),
    StructField("price", FloatType(), True)
])
lines = spark.read.schema(schema).csv("customer-orders.csv")
lines.printSchema()

lines.groupBy("customerID").agg(func.round(func.sum("price"), 2).alias("total_spent")).sort("total_spent", ascending=False).show()

spark.stop()