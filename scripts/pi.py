from pyspark.sql import SparkSession
import time

spark = SparkSession.builder.appName("LongJob").getOrCreate()
sc = spark.sparkContext

print("Starting long Spark job...")

# Make a large RDD and perform repeated operations with sleep
rdd = sc.parallelize(range(1000000))

for i in range(30):   # 30 iterations × ~10s each ≈ 5 minutes
    count = rdd.filter(lambda x: x % 2 == 0).count()
    print(f"Iteration {i}, even count = {count}")
    time.sleep(2)

print("Finished long Spark job.")
spark.stop()
