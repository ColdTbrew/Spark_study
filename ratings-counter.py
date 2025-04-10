from pyspark import SparkConf, SparkContext
import collections

#로컬에 실행하겠다
conf = SparkConf().setMaster("local").setAppName("RatingsHistogram")
# spark context 만듬
sc = SparkContext(conf = conf)

# load the data
lines = sc.textFile("./ml-100k/u.data")
ratings = lines.map(lambda x: x.split()[2]) # 컬럼 2에 레이팅이 잇음
result = ratings.countByValue()
print(result)
sortedResults = collections.OrderedDict(sorted(result.items()))
for key, value in sortedResults.items():
    print("%s %i" % (key, value))
