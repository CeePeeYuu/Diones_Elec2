from pyspark.sql import SparkSession
from pyspark.sql.functions import avg

# 1. Create Spark Session
spark = SparkSession.builder \
    .appName("WorldHappinessPartitionExample") \
    .getOrCreate()

# 2. Load Dataset
df = spark.read.csv(
    r"C:/spark/spark-3.5.8-bin-hadoop3/bin/World-happiness-report-2024.csv",
    header=True,
    inferSchema=True
)

print("Original Data")
df.show(5)

# 3. Partition Strategy 1: Repartition by Region
partitioned_df = df.repartition("Regional indicator")

print("Number of partitions after repartition:")
print(partitioned_df.rdd.getNumPartitions())

# 4. Transformation Pipeline 1
# Filter data by region
western_europe = partitioned_df.filter(
    partitioned_df["Regional indicator"] == "Western Europe"
)

# Select columns and sort by happiness score
sorted_europe = western_europe.select("Country name", "Ladder score") \
    .sort("Ladder score", ascending=False)

print("Western Europe Countries Sorted by Happiness Score")
sorted_europe.show()

# 5. Transformation Pipeline 2
# Summarize: Average happiness score per region
region_summary = partitioned_df.groupBy("Regional indicator") \
    .agg(avg("Ladder score").alias("Average Happiness Score")) \
    .sort("Average Happiness Score", ascending=False)

print("Average Happiness Score per Region")
region_summary.show()

# 6. Partition Strategy 2: Coalesce (reduce partitions)
reduced_partition = region_summary.coalesce(2)

print("Number of partitions after coalesce:")
print(reduced_partition.rdd.getNumPartitions())

# 7. Save result
reduced_partition.write.mode("overwrite").csv("happiness_output")

# Stop Spark
spark.stop()