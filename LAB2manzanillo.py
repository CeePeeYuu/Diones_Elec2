from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, count

# 1. Create Spark Session
spark = SparkSession.builder \
    .appName("FakePeoplePartitionExample") \
    .getOrCreate()

# 2. Load Dataset
df = spark.read.csv(
    r"C:/spark/spark-3.5.8-bin-hadoop3/bin/fake_people.csv",
    header=True,
    inferSchema=True
)

print("Original Data")
df.show(5)

# 3. Partition Strategy 1: Repartition by Country
partitioned_df = df.repartition("country")

print("Number of partitions after repartition:")
print(partitioned_df.rdd.getNumPartitions())

# 4. Transformation Pipeline 1
# Filter data by a specific country
canada_df = partitioned_df.filter(
    partitioned_df["country"] == "Canada"
)

# Select columns and sort by full_name
sorted_canada = canada_df.select("full_name", "city", "country") \
    .sort("full_name", ascending=True)

print("People from Canada Sorted by Full Name")
sorted_canada.show()

# 5. Transformation Pipeline 2
# Summarize: Count of people per country
country_summary = partitioned_df.groupBy("country") \
    .agg(count("id").alias("Total People")) \
    .sort("Total People", ascending=False)

print("Total People per Country")
country_summary.show()

# 6. Partition Strategy 2: Coalesce (reduce partitions)
reduced_partition = country_summary.coalesce(2)

print("Number of partitions after coalesce:")
print(reduced_partition.rdd.getNumPartitions())

# 7. Save result
reduced_partition.write.mode("overwrite").csv("fake_people_output")

# Stop Spark
spark.stop()