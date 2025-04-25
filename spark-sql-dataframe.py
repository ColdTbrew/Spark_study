from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Spark SQL").getOrCreate()

people = spark.read.csv("fakefriends-header.csv", header=True, inferSchema=True)

print("here is our inferred schema:")
people.printSchema()

print("lets display the name column")
people.select("name").show()

print("filter out anyone over 21")
people.filter(people.age < 21).show()

print("group by age and count")
people.groupBy("age").count().show()

print("make everyone 10 years older")
people.select(people.name, people.age + 10).show()

spark.stop()