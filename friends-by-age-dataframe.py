from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

# Spark 세션 초기화
spark = SparkSession.builder.appName("MinTemperatures").getOrCreate()

# 스키마 정의: measure_type을 StringType으로 수정
schema = StructType([
    StructField("stationID", StringType(), True),
    StructField("date", IntegerType(), True),
    StructField("measure_type", StringType(), True),  # IntegerType → StringType
    StructField("temperature", FloatType(), True)
])

# 데이터 로드
df = spark.read.schema(schema).csv("1800.csv")
df.printSchema()

# TMIN 필터링
minTemps = df.filter(df.measure_type == "TMIN")
stationTemps = minTemps.select("stationID", "temperature")

# 관측소별 최소 온도
minTempsByStation = stationTemps.groupBy("stationID").min("temperature")
minTempsByStation.show()

# 화씨 변환: 문법 오류 수정, 온도 단위 조정
minTempsByStationF = minTempsByStation.withColumn(
    "temperature",
    func.round(minTempsByStation["min(temperature)"] * 0.1 * (9.0/5.0) + 32, 2)
)
minTempsByStationF = minTempsByStationF.select("stationID", "temperature").sort("temperature")

# 결과 출력: collect() 대신 show() 사용 권장
minTempsByStationF.show()

# 필요 시 collect()로 출력
results = minTempsByStationF.collect()
for result in results:
    print(f"{result[0]}\t{result[1]:.2f}F")

# Spark 세션 종료
spark.stop()