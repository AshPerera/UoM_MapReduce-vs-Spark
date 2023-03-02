import time
import os
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("CarrierDelayAnalysis").config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.1").getOrCreate()

# set AWS access key and secret key as environment variables
spark.conf.set("spark.hadoop.fs.s3a.access.key", "")
spark.conf.set("spark.hadoop.fs.s3a.secret.key", "")


# Set the S3 CSV file path
s3_path = "s3a://delayedflights/DelayedFlights-updated.csv"

# Read the CSV file into a Spark DataFrame
df = spark.read.csv(s3_path, header=True, inferSchema=True)

# Register the DataFrame as a temporary table
df.createOrReplaceTempView("delay_flights")

# Run the query to get year-wise carrier delay
start_time = time.time()
result = spark.sql("SELECT Year, avg((SecurityDelay /ArrDelay)*100) from delay_flights GROUP BY Year")
end_time = time.time()

# Print the query result and execution time
result.show()
print("Query execution time: {} seconds".format(end_time - start_time))

# Stop the SparkSession
spark.stop()