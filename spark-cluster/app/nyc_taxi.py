from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder     .appName("NYC Taxi 2016")     .master("spark://spark-master:7077")     .config("spark.executor.memory", "512m")     .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

df = spark.read     .option("header", "true")     .option("inferSchema", "true")     .csv("hdfs://namenode:9000/data/taxi.csv")

print("===== SCHEMA =====")
df.printSchema()

print("===== 5 DONG DAU =====")
df.show(5, truncate=False)

print("===== TONG SO DONG =====")
print(df.count())

print("===== TRUNG BINH TIP THEO LOAI THANH TOAN =====")
df.groupBy("payment_type")   .agg(
      F.round(F.avg("tip_amount"), 2).alias("avg_tip"),
      F.round(F.avg("total_amount"), 2).alias("avg_total"),
      F.count("*").alias("so_chuyen")
  )   .orderBy("payment_type")   .show()
  
spark.stop()
