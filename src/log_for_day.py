from pyspark.sql import SparkSession
from pyspark.sql import functions
from pyspark.sql.functions import unbase64, split, udf, from_unixtime, broadcast
from pyspark.sql.types import StringType
import urllib
import sys

def ZumLogAnalyzer(input, output):

    spark = SparkSession.builder\
        .appName("spark-pilot")\
        .config("spark.master", "local[*]")\
        .getOrCreate()

    zum_log = spark.read.csv(input, inferSchema=True, header=False, sep="\t")\
        .toDF("host", "none", "user_id", "date", "http_method", "url", "http_version", \
            "status_code", "length", "referrer", "user_agent", "cookie")

    zum_log.na.drop()

    zum_log = zum_log.select("host", "url", "cookie")\
        .where(zum_log['host'] != "112.216.127.98")\
        .drop('host')\
        .filter(zum_log["cookie"].contains("_ZUID"))

    zum_log = zum_log\
        .withColumn('status', functions.regexp_extract('url', 'data=(.*)', 1))\
        .withColumn('status', functions.regexp_replace('status', '&time(.*)', ''))

    zum_log = zum_log\
        .withColumn('status', unbase64(zum_log['status']).cast("string"))\
        .drop("url")
        
    zum_log = zum_log\
        .filter(zum_log['status'].startswith('{"event":"@PageView"'))\
        .filter(zum_log['status'].contains('search.zum.com'))\
        .withColumn("time", functions.regexp_extract("status", '"time":(\d{10})', 1))

    zum_log = zum_log\
        .withColumn('status', functions.regexp_replace('status', '(.+)"url":', ''))\
        .withColumn('status', functions.regexp_replace('status', '(.+)query=', ''))\
        .withColumn('status', functions.regexp_replace('status', '[&"].*', ''))

    def decode(val):
        return urllib.parse.unquote(val.encode('utf-8'))
    decode_udf = udf(decode, StringType())

    zum_log = zum_log.withColumn("status", decode_udf("status"))\
        .withColumn("status", functions.regexp_replace("status", " ", ""))\
        .withColumn("status", functions.regexp_replace("status", "\+", ""))\
        .filter(zum_log.status != "")\
        .withColumnRenamed("status", "query")\
        .withColumn("cookie", split("cookie", "_ZUID=").getItem(1))\
        .withColumn("cookie", split("cookie", ";").getItem(0))\
        .withColumnRenamed("cookie", "id")

    zum_log = zum_log.withColumn("time", from_unixtime('time'))\
        .withColumn("time", functions.regexp_extract("time", "(\d{4}-\d{2})", 1))
    # # PV
    pv_count = zum_log.count()
    # # UV
    uv_count = zum_log.select("id").distinct().count()

    zum_log = zum_log.dropDuplicates(['id', 'query'])\
                    .sort("query")
    zum_log.cache()

    company = spark.read.csv("/Users/jungik/Downloads/company_synonym", header=False, inferSchema=True, sep="\t")\
                .toDF("market", "company", "code", "synonym")\
                .drop("code")

    suffix = spark.read.csv("/Users/jungik/Downloads/suffix_keyword").toDF("suffix")

    company = company.select("company", "synonym")
    join_company = company.select("company", "synonym")\
        .crossJoin(suffix)\
        .select("company", functions.concat("synonym", "suffix").alias("join"))\
        .union(company)\
        .sort("company", "join")

    broadcast_log = zum_log.join(broadcast(join_company), zum_log['query'] == join_company['join']).drop("query", "join")

    broadcast_log.cache()

    time_df = zum_log.select("time").distinct()
    time_list = time_df.toPandas()

    day_range = []
    for day in time_list.time:
    
        day_log = broadcast_log.where(broadcast_log['time'] == day)
        
        query_output = day_log.groupBy("company").count().orderBy("count", ascending=False)

        output_df = query_output.toPandas()
        
        output_df.to_csv(f"{output}/query-{day}")

        day_range.append(day)

    print("")
    print(f"Periods : {sorted(day_range)[0]} ~ {sorted(day_range)[-1]}")
    print(f"PV total : {pv_count}\nUV total : {uv_count}")


 
if __name__ == "__main__":
    if len(sys.argv) == 3:
        input = sys.argv[1]
        output = sys.argv[2]
        ZumLogAnalyzer(input, output)
    else:
        print("please give 2 Argument")