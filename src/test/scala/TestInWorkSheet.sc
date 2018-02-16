import java.text.SimpleDateFormat
import java.util.Locale

import org.apache.spark.sql.{Row, SparkSession}

val spark = SparkSession.builder().appName("test").master("local").getOrCreate()


val list = List(
  List[AnyRef](new SimpleDateFormat("EEE MMM dd HH:mm:ss Z yyyy", Locale.ENGLISH).parse("Sun Jul 31 10:21:53 PDT 2016"), "pm1", 11L: java.lang.Long,"ri1"),
  List[AnyRef](new SimpleDateFormat("EEE MMM dd HH:mm:ss Z yyyy", Locale.ENGLISH).parse("Mon Aug 01 12:57:09 PDT 2016"), "pm3", 5L: java.lang.Long, "ri1"),
  List[AnyRef](new SimpleDateFormat("EEE MMM dd HH:mm:ss Z yyyy", Locale.ENGLISH).parse("Mon Aug 01 01:11:16 PDT 2016"), "pm1", 1L: java.lang.Long, "ri2")
)

val rdd1 = spark.sparkContext.parallelize(list)//.map(lis => Row.fromSeq(new java.sql.Date((lis.head.asInstanceOf[java.util.Date]).getTime)::lis.tail))


