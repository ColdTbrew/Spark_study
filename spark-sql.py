from pyspark.sql import SparkSession
from pyspark.sql import Row

spark = SparkSession.builder.appName("Spark SQL").getOrCreate()

def mapper(line):
    fields = line.split(",")
    return Row(ID=fields[0],name=str(fields[1].encode("utf-8")), age=int(fields[2]), numFriends=int(fields[3]))

lines = spark.sparkContext.textFile("fakefriends.csv")
people = lines.map(mapper)

schemaPeople = spark.createDataFrame(people).cache()
schemaPeople.createOrReplaceTempView("people")

tennagers = spark.sql("SELECT * FROM people where age >= 13 and age <= 19")
for teen in tennagers.collect():
    print(teen)

schemaPeople.groupBy("age").count().orderBy("age").show()

spark.stop()