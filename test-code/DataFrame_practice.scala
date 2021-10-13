// DataFrame Practice


scala> import org.apache.spark.sql.SparkSession
scala> val spark = SparkSession.builder().getOrCreate()


import spark.implicits._




// 데이터 로드 // 

scala> val itPostsRows = sc.textFile("italianPosts.csv")
itPostsRows: org.apache.spark.rdd.RDD[String] = first-edition/ch05/italianPosts.csv MapPartitionsRDD[1] at textFile at <console>:30

scala> val itPostsSplit = itPostsRows.map(x => x.split("~"))
itPostsSplit: org.apache.spark.rdd.RDD[Array[String]] = MapPartitionsRDD[2] at map at <console>:3

scala> val itPostsRDD = itPostsSplit.map(x => (x(0),x(1),x(2),x(3),x(4),x(5),x(6),x(7),x(8),x(9),x(10),x(11),x(12)))
itPostsRDD: org.apache.spark.rdd.RDD[(String, String, String, String, String, String, String, String, String, String, String, String, String)] = MapPartitionsRDD[3] at map at <console>:34


scala> val itPostsDFrame = itPostsRDD.toDF()


scala> itPostsDFrame.show(10)



scala> val itPostsDF = itPostsRDD.toDF("commentCount", "lastActivityDate", "ownerUserId", "body", "score", "creationDate", "viewCount", "title", "tags", "answerCount", "acceptedAnswerId", "postTypeId", "id")


scala> itPostsDF.show(10)


scala> itPostsDF.printSchema



// 케이스 클래스를 사용해 RDD를 DataFrame으로 변환 // 

import java.sql.Timestamp

case class Post (commentCount:Option[Int], lastActivityDate:Option[java.sql.Timestamp],
  ownerUserId:Option[Long], body:String, score:Option[Int], creationDate:Option[java.sql.Timestamp],
  viewCount:Option[Int], title:String, tags:String, answerCount:Option[Int],
  acceptedAnswerId:Option[Long], postTypeId:Option[Long], id:Long)

object StringImplicits {
   implicit class StringImprovements(val s: String) {
      import scala.util.control.Exception.catching
      def toIntSafe = catching(classOf[NumberFormatException]) opt s.toInt
      def toLongSafe = catching(classOf[NumberFormatException]) opt s.toLong
      def toTimestampSafe = catching(classOf[IllegalArgumentException]) opt Timestamp.valueOf(s)
   }
}

import StringImplicits._
def stringToPost(row:String):Post = {
  val r = row.split("~")
  Post(r(0).toIntSafe,
    r(1).toTimestampSafe,
    r(2).toLongSafe,
    r(3),
    r(4).toIntSafe,
    r(5).toTimestampSafe,
    r(6).toIntSafe,
    r(7),
    r(8),
    r(9).toIntSafe,
    r(10).toLongSafe,
    r(11).toLongSafe,
    r(12).toLong)
}


val itPostsDFCase = itPostsRows.map(x => stringToPost(x)).toDF()
itPostsDFCase.printSchema





// 스키마를 지정해 RDD를 DataFrame으로 변환 // 
import org.apache.spark.sql.types._
val postSchema = StructType(Seq(
  StructField("commentCount", IntegerType, true),
  StructField("lastActivityDate", TimestampType, true),
  StructField("ownerUserId", LongType, true),
  StructField("body", StringType, true),
  StructField("score", IntegerType, true),
  StructField("creationDate", TimestampType, true),
  StructField("viewCount", IntegerType, true),
  StructField("title", StringType, true),
  StructField("tags", StringType, true),
  StructField("answerCount", IntegerType, true),
  StructField("acceptedAnswerId", LongType, true),
  StructField("postTypeId", LongType, true),
  StructField("id", LongType, false))
  )

import org.apache.spark.sql.Row
def stringToRow(row:String):Row = {
  val r = row.split("~")
  Row(r(0).toIntSafe.getOrElse(null),
    r(1).toTimestampSafe.getOrElse(null),
    r(2).toLongSafe.getOrElse(null),
    r(3),
    r(4).toIntSafe.getOrElse(null),
    r(5).toTimestampSafe.getOrElse(null),
    r(6).toIntSafe.getOrElse(null),
    r(7),
    r(8),
    r(9).toIntSafe.getOrElse(null),
    r(10).toLongSafe.getOrElse(null),
    r(11).toLongSafe.getOrElse(null),
    r(12).toLong)
}


val rowRDD = itPostsRows.map(row => stringToRow(row))
val itPostsDFStruct = spark.createDataFrame(rowRDD, postSchema)


// 스키마 정보 가져오기  // 
scala> itPostsDFStruct.columns
scala> itPostsDFStruct.dtypes




// 기본 DataFrame API // 
// 칼럼선택 // 

scala> val postsDf = itPostsDFStruct
scala> val postsIdBody = postsDf.select("id", "body")

scala> val postsIdBody = postsDf.select(postsDf.col("id"), postsDf.col("body"))

scala> val postsIdBody = postsDf.select(Symbol("id"), Symbol("body"))

scala> val postsIdBody = postsDf.select('id, 'body)

scala> val postsIdBody = postsDf.select($"id", $"body")


scala> val postIds = postsIdBody.drop("body")


// 데이터필터링 // 

scala> postsIdBody.filter('body contains "Italiano").count()


scala> val noAnswer = postsDf.filter(('postTypeId === 1) and ('acceptedAnswerId isNull))

scala> val firstTenQs = postsDf.filter('postTypeId === 1).limit(10)


// 칼럼을 추가하거나 칼럼 이름 변경 // 
scala> val firstTenQsRn = firstTenQs.withColumnRenamed("ownerUserId", "owner")


scala> postsDf.filter('postTypeId === 1).withColumn("ratio", 'viewCount / 'score).where('ratio < 35).show()


scala> postsDf.filter('postTypeId === 1).orderBy('lastActivityDate desc).limit(10).show




// SQL 함수로 데이터에 연산 수행 // 

import org.apache.spark.sql.functions._


postsDf.filter('postTypeId === 1).withColumn("activePeriod", datediff('lastActivityDate, 'creationDate)).orderBy('activePeriod desc).head.getString(3).replace("&lt;","<").replace("&gt;",">")


scala> postsDf.select(avg('score), max('score), count('score)).show








// 윈도 함수 // 


import org.apache.spark.sql.expressions.Window



postsDf.filter('postTypeId === 1).select('ownerUserId, 'acceptedAnswerId, 'score, max('score).over(Window.partitionBy('ownerUserId)) as "maxPerUser").withColumn("toMax", 'maxPerUser - 'score).show(10)


postsDf.filter('postTypeId === 1).select('ownerUserId, 'id, 'creationDate, lag('id, 1).over(Window.partitionBy('ownerUserId).orderBy('creationDate)) as "prev", lead('id, 1).over(Window.partitionBy('ownerUserId).orderBy('creationDate)) as "next").orderBy('ownerUserId, 'id).show()


// 사용자 정의 함수 // 

val countTags = udf((tags: String) => "&lt;".r.findAllMatchIn(tags).length)

val countTags = spark.udf.register("countTags", (tags: String) => "&lt;".r.findAllMatchIn(tags).length)

postsDf.filter('postTypeId === 1).select('tags, countTags('tags) as "tagCnt").show(10, false)






// 결측 값 다루기 // 


val cleanPosts = postsDf.na.drop()

cleanPosts.count()


postsDf.na.fill(Map("viewCount" -> 0))


val postsDfCorrected = postsDf.na.replace(Array("id", "acceptedAnswerId"), Map(1177 -> 3000))





// DataFrame을 RDD로 변환 // 


val postsRdd = postsDf.rdd


val postsMapped = postsDf.rdd.map(row => Row.fromSeq(
  row.toSeq.updated(3, row.getString(3).replace("&lt;","<").replace("&gt;",">")).
    updated(8, row.getString(8).replace("&lt;","<").replace("&gt;",">"))))


val postsDfNew = spark.createDataFrame(postsMapped, postsDf.schema)



// 데이터 그룹핑 // 

postsDfNew.groupBy('ownerUserId, 'tags, 'postTypeId).count.orderBy('ownerUserId desc).show(10)


postsDfNew.groupBy('ownerUserId).agg(max('lastActivityDate), max('score)).show(10)


postsDfNew.groupBy('ownerUserId).agg(Map("lastActivityDate" -> "max", "score" -> "max")).show(10)


postsDfNew.groupBy('ownerUserId).agg(max('lastActivityDate), max('score).gt(5)).show(10)



val smplDf = postsDfNew.where('ownerUserId >= 13 and 'ownerUserId <= 15)
smplDf.groupBy('ownerUserId, 'tags, 'postTypeId).count.show()


smplDf.rollup('ownerUserId, 'tags, 'postTypeId).count.show()


smplDf.cube('ownerUserId, 'tags, 'postTypeId).count.show()




spark.sql("SET spark.sql.caseSensitive=true")
spark.conf.set("spark.sql.caseSensitive", "true")




// 데이터 조인 // 

val itVotesRaw = sc.textFile("italianVotes.csv").map(x => x.split("~"))


val itVotesRows = itVotesRaw.map(row => Row(row(0).toLong, row(1).toLong, row(2).toInt, Timestamp.valueOf(row(3))))


val votesSchema = StructType(Seq(
  StructField("id", LongType, false),
  StructField("postId", LongType, false),
  StructField("voteTypeId", IntegerType, false),
  StructField("creationDate", TimestampType, false))
  )



val votesDf = spark.createDataFrame(itVotesRows, votesSchema)

val postsVotes = postsDf.join(votesDf, postsDf("id") === votesDf("postId"))
val postsVotesOuter = postsDf.join(votesDf, postsDf("id") === votesDf("postId"), "outer")



// 테이블 카탈로그와 하이브 메타스토어 // 

postsDf.createOrReplaceTempView("posts_temp")
postsDf.write.saveAsTable("posts")
votesDf.write.saveAsTable("votes")

spark.catalog.listTables().show()
spark.catalog.listColumns("votes").show()
spark.catalog.listFunctions.show()


// SQL 쿼리 실행 // 


val resultDf = sql("select * from posts")

spark-sql> select substring(title, 0, 70) from posts where postTypeId = 1 order by creationDate desc limit 3;


$ spark-sql -e "select substring(title, 0, 70) from posts where postTypeId = 1 order by creationDate desc limit 3"





// 쓰리프트 서버로 스파크 SQL 접속 // 


/opt/spark/sbin/start-thriftserver.sh



beeline -u jdbc:hive2://192.168.56.1:10000

0: jdbc:hive2://192.168.56.1:10000> show tables;



// 데이터 저장 // 


postsDf.write.format("json").saveAsTable("postsjson")

sql("select * from postsjson")

val props = new java.util.Properties()
props.setProperty("user", "user")
props.setProperty("password", "password")
postsDf.write.jdbc("jdbc:postgresql://postgresrv/mydb", "posts", props)





// 데이터 불러오기 // 

val postsDf = spark.read.table("posts")
val postsDf = spark.table("posts")

val result = spark.read.jdbc("jdbc:postgresql://postgresrv/mydb", "posts", Array("viewCount > 3"), props)

// SQL 메서드로 등록한 데이터 소스에서 데이터 불러오기 // 

sql("CREATE TEMPORARY TABLE postsjdbc "+
  "USING org.apache.spark.sql.jdbc "+
  "OPTIONS ("+
    "url 'jdbc:postgresql://postgresrv/mydb',"+
    "dbtable 'posts',"+
    "user 'user',"+
    "password 'password')")

val result = sql("select * from postsjdbc")


sql("CREATE TEMPORARY TABLE postsParquet "+
  "USING org.apache.spark.sql.parquet "+
  "OPTIONS (path '/path/to/parquet_file')")

val resParq = sql("select * from postsParquet")
