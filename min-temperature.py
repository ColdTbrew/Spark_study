from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("MinTemperatures")
sc = SparkContext(conf = conf)

def parseLine(line):
    fields = line.split(',')
    stationID = fields[0]
    entryType = fields[2]
    temperature = float(fields[3]) * 0.1 * (9.0/5.0) + 32
    return (stationID, entryType, temperature)
# 1. 파일 읽기
lines = sc.textFile('1800.csv')
print("lines:")
for line in lines.take(5):
    print(line)

# 2. 파싱
parsedLines = lines.map(parseLine)
print("\nparsedLines:")
for parsed in parsedLines.take(5):
    print(parsed)

# 3. TMIN 필터링
minTemps = parsedLines.filter(lambda x: "TMIN" in x[1])
print("\nminTemps (after filter):")
for temp in minTemps.take(5):
    print(temp)

# 4. stationID와 temperature만 추출
stationTemps = minTemps.map(lambda x: (x[0], x[2]))
print("\nstationTemps:")
for station_temp in stationTemps.take(5):
    print(station_temp)

# 5. station별 최소 온도 계산
minTemps = stationTemps.reduceByKey(lambda x, y: min(x, y))
print("\nminTemps (after reduceByKey):")
for result in minTemps.take(5):
    print(result)

# 최종 결과 출력
results = minTemps.collect()
print("\nFinal Results:")
for result in results:
    print(f"{result[0]}\t{result[1]:.2f}F")

# SparkContext 종료
sc.stop()