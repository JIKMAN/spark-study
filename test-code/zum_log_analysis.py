from pyspark.sql import SparkSession
from pyspark.sql import functions
from pyspark.sql.functions import unbase64, split, udf, encode
from pyspark.sql import Column
from pyspark.sql.types import StringType

spark = SparkSession.builder.appName("spark-pilot").config("spark.master", "local").getOrCreate()

zum_log = spark.read.csv("/Users/user/JIKMAN/spark/log.tsv", inferSchema=True, header=False, sep="\t")\
    .toDF("host", "none", "user_id", "date", "http_method", "url", "http_version", "status_code", "length", "referrer", "user_agent", "cookie")

zum_log.na.drop()

zum_log.count()
