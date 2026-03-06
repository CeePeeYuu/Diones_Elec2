from pyspark.sql import SparkSession
from pyspark.sql.functions import avg

# 1. Create Spark Session
spark = SparkSession.builder \
    .appName("COVID19BPSPartitionExample") \
    .getOrCreate()

# 2. Load Dataset
df = spark.read.csv(
    r"C:/spark/spark-3.5.8-bin-hadoop3/bin/Refined_COVID19_BPS.csv",
    header=True,
    inferSchema=True
)

print("Original Data")
df.show(5)

# 3. Partition Strategy 1: Repartition by Breakdown_Type
partitioned_df = df.repartition("Breakdown_Type")

print("Number of partitions after repartition:")
print(partitioned_df.rdd.getNumPartitions())

# 4. Transformation Pipeline 1
# Filter data by Firm Size breakdown
firm_size_df = partitioned_df.filter(
    partitioned_df["Breakdown_Type"] == "Firm Size"
)

# Select columns and sort by Percentage
sorted_firm = firm_size_df.select("Country", "Breakdown_Category", "Percentage") \
    .sort("Percentage", ascending=False)

print("Firm Size Breakdown Sorted by Percentage")
sorted_firm.show()

# 5. Transformation Pipeline 2
# Summarize: Average Percentage per Country
country_summary = partitioned_df.groupBy("Country") \
    .agg(avg("Percentage").alias("Average Percentage")) \
    .sort("Average Percentage", ascending=False)

print("Average Percentage per Country")
country_summary.show()

# 6. Partition Strategy 2: Coalesce (reduce partitions)
reduced_partition = country_summary.coalesce(2)

print("Number of partitions after coalesce:")
print(reduced_partition.rdd.getNumPartitions())

# 7. Save result
reduced_partition.write.mode("overwrite").csv("covid19_bps_output")

# Stop Spark
spark.stop()