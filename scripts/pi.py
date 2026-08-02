import argparse
import time

from pyspark.sql import SparkSession


def parse_args():
    parser = argparse.ArgumentParser(description="Retry-friendly PySpark demo job")
    parser.add_argument("--iterations", type=int, default=30)
    parser.add_argument("--sleep-seconds", type=float, default=2.0)
    parser.add_argument("--records", type=int, default=1_000_000)
    args = parser.parse_args()
    if args.iterations < 1 or args.sleep_seconds < 0 or args.records < 1:
        parser.error(
            "iterations and records must be positive; sleep must be non-negative"
        )
    return args


def main():
    args = parse_args()
    spark = SparkSession.builder.appName("IdempotencyDemo").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    try:
        print(
            f"Starting Spark demo: iterations={args.iterations}, records={args.records}"
        )
        rdd = spark.sparkContext.parallelize(range(args.records))
        for iteration in range(args.iterations):
            count = rdd.filter(lambda value: value % 2 == 0).count()
            print(f"Iteration {iteration + 1}, even count = {count}")
            time.sleep(args.sleep_seconds)
        print("Finished Spark demo job.")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
