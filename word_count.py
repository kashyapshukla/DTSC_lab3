from pyspark.sql import SparkSession
import re

# Initialize SparkSession with AWS Credentials
spark = SparkSession \
    .builder \
    .appName("UniqueWordCount") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.2.0") \
    .getOrCreate()

# Configure AWS Access Keys
spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", "bigData_lab3#.ppk")
spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "bigData_lab3#.ppk")

# Read data from S3 bucket
input_path = "s3a://aws-logs-310188540700-us-east-1/elasticmapreduce/j-2QAQT3GR6NOJC/faculty_bios.txt"
data = spark.read.text(input_path).rdd.map(lambda x: x[0])

# Define the MapReduce job
def word_count_map(line):
    words = re.findall(r'\b\w+\b', line.lower())
    return [(word, 1) for word in words]

word_counts = data.flatMap(word_count_map).reduceByKey(lambda a, b: a + b)

# Save output to S3 bucket
output_path = "s3://aws-logs-310188540700-us-east-1/elasticmapreduce/j-2QAQT3GR6NOJC/word_count.txt"
word_counts.saveAsTextFile(output_path)

spark.stop()