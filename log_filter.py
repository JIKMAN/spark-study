from pyspark.sql import SparkSession
from pyspark.sql import functions
from pyspark.sql.functions import unbase64, split, udf, broadcast
from pyspark.sql.types import StringType
import urllib
import sys

def ZumLogAnalyzer(input_path, output_path):

    spark = SparkSession.builder\
        .appName("spark-pilot")\
        .config("spark.master", "local[*]")\
        .getOrCreate()

    load_log = spark.read.csv(input_path, inferSchema=True, header=False, sep="\t")\
        .toDF("host", "none", "user_id", "date", "http_method", "url", "http_version", \
            "status_code", "length", "referrer", "user_agent", "cookie")

    load_log = load_log.na.drop()

    selected_log = load_log.select("host", "url", "cookie")\
        .where(load_log['host'] != "112.216.127.98")\
        .drop('host')\
        .filter(load_log["cookie"].contains("_ZUID"))

    status_log = selected_log\
        .withColumn('status', functions.regexp_extract('url', 'data=(.*)', 1))\
        .withColumn('status', functions.regexp_replace('status', '&time(.*)', ''))

    base64_decoded_log = status_log\
        .withColumn('status', unbase64(status_log['status']).cast("string"))\
        .drop("url")
        
    event_log = base64_decoded_log\
        .filter(base64_decoded_log['status'].startswith('{"event":"@PageView"'))\
        .filter(base64_decoded_log['status'].contains('search.zum.com'))

    event_query_log = event_log\
        .withColumn('status', functions.regexp_replace('status', '(.+)"url":', ''))\
        .withColumn('status', functions.regexp_replace('status', '(.+)query=', ''))\
        .withColumn('status', functions.regexp_replace('status', '[&"].*', ''))

    def decode(val):
        return urllib.parse.unquote(val.encode('utf-8'))
    decode_udf = udf(decode, StringType())

    cleaned_zum_log = event_query_log.withColumn("status", decode_udf("status"))\
        .withColumn("status", functions.regexp_replace("status", " ", ""))\
        .withColumn("status", functions.regexp_replace("status", "\+", ""))\
        .filter(event_query_log.status != "")\
        .withColumnRenamed("status", "query")\
        .withColumn("cookie", split("cookie", "_ZUID=").getItem(1))\
        .withColumn("cookie", split("cookie", ";").getItem(0))\
        .withColumnRenamed("cookie", "id")

    cleaned_zum_log.cache()
    # # PV
    pv_count = cleaned_zum_log.count()
    # # UV
    uv_count = cleaned_zum_log.select("id").distinct().count()

    cleaned_zum_log = cleaned_zum_log.dropDuplicates(['id', 'query'])\
                    .sort("query")
    

    company_data = spark.read.csv("/Users/jungik/Downloads/company_synonym", header=False, inferSchema=True, sep="\t")\
                .toDF("market", "company", "code", "synonym")\
                .drop("code")

    suffix = spark.read.csv("/Users/jungik/Downloads/suffix_keyword").toDF("suffix")

    selected_company = company_data.select("company", "synonym")
    joined_company = selected_company.select("company", "synonym")\
        .crossJoin(suffix)\
        .select("company", functions.concat("synonym", "suffix").alias("join"))\
        .union(selected_company)\
        .sort("company", "join")

    zum_log_joined_company = cleaned_zum_log.join(broadcast(joined_company), cleaned_zum_log['query'] == joined_company['join']).drop("query", "join")
        
    zum_log_output = zum_log_joined_company.groupBy("company").count().orderBy("count", ascending=False)

    zum_log_output.coalesce(1).write.format("csv")\
        .option("header", "false")\
        .mode("append")\
        .save(output_path)

    print(f"PV : {pv_count}")
    print(f"UV : {uv_count}")

 
if __name__ == "__main__":
    if len(sys.argv) == 3:
        input_path = sys.argv[1]
        output_path = sys.argv[2]
        ZumLogAnalyzer(input_path, output_path)
    else:
        print("please give 2 Argument")