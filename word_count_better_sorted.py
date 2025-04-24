import re
from pyspark import SparkContext, SparkConf

def normalizeWords(text):
    return re.compile(r'\W+').split(text.lower())
    # \W+ : 모든 문자(숫자, 알파벳, _)를 제외한 모든 문자
    # lower() : 소문자로 변환

conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf = conf)

input = sc.textFile("Book.txt")
words = input.flatMap(normalizeWords)
wordCounts = words.map(lambda x: (x, 1)).reduceByKey(lambda x, y: x + y)
wordCountsSorted = wordCounts.map(lambda pair: (pair[1], pair[0])).sortByKey()
result = wordCountsSorted.collect()

for count,word in result:
    cleanWord = word.encode("ascii", "ignore")
    if cleanWord:
        print(f"{word}: {count}")