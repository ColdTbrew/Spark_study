from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("FriendsByAge")
sc = SparkContext(conf = conf)

def parseline(line):
    fields = line.split(',')
    age = int(fields[2])
    numFriends = int(fields[3])
    return (age, numFriends)

lines = sc.textFile("./fakefriends.csv")

rdd = lines.map(parseline)

print("--------")
totalsByAge = rdd.mapValues(lambda x: (x, 1)).reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))
averageByAge = totalsByAge.mapValues(lambda x : x[0]/ x[1])
results = averageByAge.collect() #collect(): RDD의 모든 데이터를 드라이버로 모아서 리스트로 반환하는 액션 함수.

for r in sorted(results):
    print(r[0], round(r[1],2))
