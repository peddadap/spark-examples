from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, TimestampType
from pyspark.sql.functions import col, sum as spark_sum
import os
import psutil  # To get total system memory
from pathlib import Path

# Step 1: Discover total system resources

# Get total cores on the local machine
total_cores = os.cpu_count()

# Reserve 70% of cores for Spark (30% for OS)
usable_cores = max(1, int(total_cores * 0.7))

# Get total memory on the local machine (in bytes)
total_memory_bytes = psutil.virtual_memory().total

# Convert total memory to GB
total_memory_gb = total_memory_bytes / (1024 ** 3)

# Reserve 50% of memory for Spark (other 50% for OS and other processes)
usable_memory_gb = total_memory_gb * 0.5

# Step 2: Define driver and executor configurations

# Split resources between driver and executors based on workload distribution

# 1. Driver Configuration
# Allocate 10-20% of usable memory to the driver
driver_memory_gb = max(1, int(usable_memory_gb * 0.15))  # 15% of usable memory

# Allocate 10-20% of usable cores to the driver
driver_cores = max(1, int(usable_cores * 0.15))  # 15% of usable cores

# 2. Executor Configuration
# Remaining cores for executors
executor_cores = usable_cores - driver_cores

# Set cores per executor (default to 2 cores per executor)
default_cores_per_executor = 2
num_executors = max(1, executor_cores // default_cores_per_executor)

# Convert usable memory to MB for executor calculations
usable_memory_mb = usable_memory_gb * 1024

# Allocate memory to executors after subtracting driver memory
executor_memory_mb = max(1, usable_memory_mb - (driver_memory_gb * 1024))

# Calculate memory per executor
base_memory_per_executor = executor_memory_mb // num_executors

# Set memory overhead for executors (10% of executor memory or minimum of 384 MB)
memory_overhead_mb = max(384, int(0.1 * base_memory_per_executor))

# Calculate effective memory for executors
effective_executor_memory_mb = base_memory_per_executor - memory_overhead_mb
effective_executor_memory_gb = max(1, int(effective_executor_memory_mb // 1024))  # Convert to GB

# Step 3: Initialize Spark Session with adjusted configurations
spark = SparkSession.builder \
    .appName("ClusterConfigTest") \
    .master("local[*]") \
    .config("spark.network.timeout", "61s") \
    .config("spark.driver.bindAddress", "127.0.0.1") \
    .config("spark.executor.heartbeatInterval", "60s") \
    .config("spark.rpc.askTimeout", "300s") \
    .config("spark.local.ip", "127.0.0.1") \
    .config("spark.executor.instances", num_executors) \
    .config("spark.executor.cores", default_cores_per_executor) \
    .config("spark.executor.memory", f"{effective_executor_memory_gb}g") \
    .config("spark.executor.memoryOverhead", f"{memory_overhead_mb}m") \
    .config("spark.sql.shuffle.partitions", executor_cores * 2) \
    .config("spark.driver.memory", f"{driver_memory_gb}g") \
    .config("spark.driver.cores", driver_cores) \
    .config("spark.driver.maxResultSize", "1g") \
    .getOrCreate()

print("Spark session started with adjusted driver and executor configurations.")

# Print the configuration for verification
print("\nCluster Configuration:")
print(f"Total Cores: {total_cores}")
print(f"Usable Cores for Spark: {usable_cores}")
print(f"Driver Memory: {driver_memory_gb}g")
print(f"Driver Cores: {driver_cores}")
print(f"Number of Executors: {num_executors}")
print(f"Executor Cores per Executor: {default_cores_per_executor}")
print(f"Executor Memory: {effective_executor_memory_gb}g")
print(f"Memory Overhead: {memory_overhead_mb}m")


# Step 4: Define the input directory path (where large files are stored)
input_dir = Path(os.getcwd(), "data")

# Step 5: Define schema for the streaming dataset
schema = StructType([
    StructField("Transaction_ID", IntegerType(), True),
    StructField("Timestamp", TimestampType(), True),
    StructField("Account_Number", StringType(), True),
    StructField("Transaction_Type", StringType(), True),
    StructField("Transaction_Amount", DoubleType(), True),
    StructField("Currency", StringType(), True),
    StructField("Location", StringType(), True),
    StructField("Merchant", StringType(), True),
    StructField("Card_Type", StringType(), True),
    StructField("Approval_Status", StringType(), True)
])

# Step 5: Read streaming data from the input directory
input_dir = Path(os.getcwd(), "data")  # Update this path with your streaming data location
df = spark.readStream \
    .schema(schema) \
    .format("csv") \
    .option("header", "true") \
    .option("maxFilesPerTrigger", 5) \
    .load(str(input_dir))

# Step 6: Perform incremental grouping by 'Transaction_Type' and sum 'Transaction_Amount'
grouped_df = df.groupBy("Transaction_Type") \
    .agg(spark_sum("Transaction_Amount").alias("Total_Transaction_Amount"))

# Step 7: Set up checkpointing and write to console
checkpoint_dir = Path(os.getcwd(), "checkpoint")

query = grouped_df.writeStream \
    .outputMode("update") \
    .format("console") \
    .option("truncate", "false") \
    .option("checkpointLocation", checkpoint_dir) \
    .trigger(processingTime="10 seconds") \
    .start()

# Wait for the streaming query to finish
query.awaitTermination()