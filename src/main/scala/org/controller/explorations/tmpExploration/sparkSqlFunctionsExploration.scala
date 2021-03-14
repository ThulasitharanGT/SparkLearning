package org.controller.explorations.tmpExploration

import org.util.SparkOpener
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
object sparkSqlFunctionsExploration extends SparkOpener{
def main(args:Array[String]):Unit ={
  val spark=SparkSessionLoc()
  import spark.implicits._
  val df=Seq(("str","2020-01-02","2021-01-02 12:14:45",3,4.5)
    ,("str","2020-01-02","2021-01-02 12:14:45",4,2.3)
    ,("str","2020-01-02","2021-01-02 12:14:45",2,8.7)
  ).toDF("strTmp","dateTmp","timestampTmp","numTmp","doubleTmp")
  df.select(add_months(col("dateTmp"),2)).show(false)
  df.select(add_months(col("timestampTmp"),2)).show(false)
  // df.select(add_months(col("timestampTmp"),col("numTmp"))).show(false)

  df.withColumn("currentDate",current_date).show(false)
  df.withColumn("currentTimestamp",current_timestamp).show(false)

  df.agg(approx_count_distinct("numTmp").as("approxDistCountNum")).show(false)

  val df2= Seq((Array("tmp1","tmp2","tmp3"),Array("tmp4","tmp2","tmp3"),Array(2,3,1),3,"tmp")
    ,(Array("tmp2","tmp2","tmp3"),Array("tmp6","tmp2","tmp3"),Array(7,4,1),4,"tmp")
    ,(Array("tmp1","tmp2","tmp2"),Array("tmp8","tmp8","tmp9"),Array(8,3,1),2,"tmp")
  ).toDF("arrStr1","arrStr2","arrInt","numTmp","tmpConstant")

  df2.withColumn("arrayContains",array_contains(col("arrInt"),col("numTmp"))).show(false)

  df2.withColumn("arrayContains",array_contains(col("arrStr1"),when(col("tmpConstant").isNotNull , concat(col("tmpConstant"),lit("2"))).otherwise(concat(col("tmpConstant"),lit("1"))))).withColumn("tmpValue",when(col("tmpConstant").isNotNull , concat(col("tmpConstant"),lit("2"))).otherwise(concat(col("tmpConstant"),lit("1")))).show(false)

  df2.withColumn("arrayDistinct",array_distinct(col("arrInt"))).withColumn("arrayDistinct2",array_distinct(col("arrStr1"))).withColumn("arrayDistinct3",array_distinct(col("arrStr2"))).show(false)

  df2.withColumn("arrayExcept",	array_except(col("arrStr1"),col("arrStr2"))).show(false)
  df2.withColumn("arrayIntersect",	array_intersect(col("arrStr1"),col("arrStr2"))).show(false)

  df2.withColumn("arrayJoin",	array_join(col("arrStr1"),"~")).show(false)
  df2.withColumn("arrayJoin",	array_join(col("arrInt"),"~")).show(false)
  df2.withColumn("arrayJoin",	array_join(col("arrInt"),",")).show(false)
  df2.withColumn("arrayMax",	array_max(col("arrInt"))).show(false)
  df2.withColumn("arrayMax",	array_max(col("arrStr1"))).show(false)
  df2.withColumn("arrayMin",	array_min(col("arrInt"))).show(false)
  df2.withColumn("arrayMin",	array_min(col("arrStr1"))).show(false)
  df2.withColumn("arrayPosition",	array_position(col("arrStr1"),"temp1")).show(false)
  df2.withColumn("arrayPosition",	array_position(col("arrInt"),2)).show(false)

  df2.withColumn("arrayRemove",	array_remove(col("arrInt"),22)).show(false)
  df2.withColumn("arrayRemove",	array_remove(col("arrInt"),2)).show(false)
  df2.withColumn("arrayRemove",	array_remove(col("arrStr1"),"temp1")).show(false)
  df2.withColumn("arrayRemove",	array_remove(col("arrStr1"),"tmp1")).show(false)
  df2.withColumn("arrayRepeat",	array_repeat(col("arrStr1"),col("numTmp"))).show(false)

  df2.withColumn("arrayRepeat",	array_repeat(col("tmpConstant"),col("numTmp"))).show(false) // argument 2 requires int type Column
  df.withColumn("arrayRepeat",	array_repeat(col("timestampTmp"),round(col("doubleTmp")).cast(IntegerType))).show(false)
  df.withColumn("arrayRepeat",	array_repeat(col("timestampTmp"),floor(col("doubleTmp")).cast(IntegerType))).show(false)
  df.withColumn("arrayRepeat",	array_repeat(col("timestampTmp"),ceil(col("doubleTmp")).cast(IntegerType))).show(false)
  df.withColumn("arrayRepeat",	array_repeat(col("timestampTmp"),5)).show(false)

  df2.withColumn("arraySort",	array_sort(col("arrStr1"))).show(false)
  df2.withColumn("arraySort",	array_sort(col("arrInt"))).show(false)

  df2.withColumn("arrayUnion",	array_union(col("arrStr2"),col("arrStr1"))).show(false) // both columns must be arrays of same datatype, dupes will be removed after merging

  df.withColumn("arrayTmp",array(col("strTmp"),col("dateTmp"),col("timestampTmp"),col("numTmp"),col("doubleTmp"))).show(false)
  df.withColumn("arrayTmp",array(col("strTmp"))).show(false)
  df.withColumn("arrayTmp",array("strTmp","dateTmp","timestampTmp","numTmp","doubleTmp")).show(false)
  df.withColumn("arrayTmp",array("strTmp","dateTmp")).show(false)

  df2.withColumn("arrayOverlaps",arrays_overlap(col("arrStr1"),col("arrStr2"))).show(false)

  df2.withColumn("arrayZip",arrays_zip(col("arrStr1"),col("arrStr2"))).show(false)


  df2.withColumn("arrayNullsFirst",asc_nulls_first("arrStr1")).show(false) // not working

  df2.withColumn("arrayNullsLast",asc_nulls_last("arrStr1")).show(false)  // not working

  df.withColumn("arrayAddDate",date_add(col("dateTmp"),5)).show(false)

  df.withColumn("arrayAddMonths",add_months(col("dateTmp"),5)).show(false)


  df.withColumn("arrayTimeStamp",add_months(col("timestampTmp"),5)).show(false)
  df.withColumn("arrayTimeStamp",add_months(col("timestampTmp"),3)).show(false)
  spark.conf.set("spark.sql.legacy.timeParserPolicy","LEGACY")
  df.withColumn("arrayTimeStamp",date_format(to_date(col("dateTmp")),"YYYY-MM-DD")).show(false)

  df.withColumn("arrayTimeStamp",to_date(col("dateTmp"))).show(false)

  df.withColumn("arraySubDate",date_sub(col("dateTmp"),5)).show(false)
  df.withColumn("arraySubDate",add_months(col("dateTmp"),5)).show(false)

  df.withColumn("arraySubDate",add_months(to_timestamp(col("timestampTmp")),5)).show(false)

  df.withColumn("arrayDateTrunc",date_trunc("YYYY-MM-DD HH:mm:ss",to_timestamp(col("timestampTmp")))) // returns null

  df.withColumn("arrayDateDiff",datediff(col("dateTmp"),col("timestampTmp"))).show(false)

  df.withColumn("arrayDayOfMonth",dayofmonth(col("dateTmp"))).show(false)

  df.withColumn("arrayDayOfMonth",dayofmonth(to_date(col("dateTmp")))).show(false)

  df.withColumn("arrayDayOfMonth",dayofmonth(to_timestamp(col("timestampTmp")))).show(false)

  df.withColumn("arrayDayOfMonth",dayofmonth(col("timestampTmp"))).show(false)

  df.withColumn("arrayDayOfMonth",dayofmonth(col("timestampTmp").cast(TimestampType))).show(false)

  df.withColumn("arrayDayOfWeek",dayofweek(col("dateTmp"))).show(false)

  df.withColumn("arrayDayOfWeek",dayofweek(to_date(col("dateTmp")))).show(false)

  df.withColumn("arrayDayOfWeek",dayofweek(to_timestamp(col("timestampTmp")))).show(false)

  df.withColumn("arrayDayOfWeek",dayofweek(col("timestampTmp"))).show(false)

  df.withColumn("arrayDayOfWeek",dayofweek(col("timestampTmp").cast(TimestampType))).show(false)

  df.withColumn("arrayDayOfYear",dayofyear(col("dateTmp"))).show(false)

  df.withColumn("arrayDayOfYear",dayofyear(to_date(col("dateTmp")))).show(false)

  df.withColumn("arrayDayOfYear",dayofyear(to_timestamp(col("timestampTmp")))).show(false)

  df.withColumn("arrayDayOfYear",dayofyear(col("timestampTmp"))).show(false)

  df.withColumn("arrayDayOfYear",dayofyear(col("timestampTmp").cast(TimestampType))).show(false)

  df2.withColumn("arrayElementAt",element_at(col("arrStr1"),col("numTmp"))).show(false)
  df2.withColumn("arrayElementAt",element_at(col("arrStr1"),2)).show(false)
  df2.withColumn("arrayElementAt",element_at(col("arrStr1"),4)).show(false)

  df2.withColumn("encodeTemp", encode(col("tmpConstant"), "US-ASCII")).show(false) // 'US-ASCII', 'ISO-8859-1', 'UTF-8', 'UTF-16BE', 'UTF-16LE', 'UTF-16'

  df2.withColumn("expTemp", exp(col("numTmp"))).show(false)
  df.withColumn("powTemp", pow(col("doubleTmp"),col("numTmp"))).show(false)

  df.withColumn("quarterTemp",quarter(col("timestampTmp"))).show(false)
  df2.withColumn("radiansTemp",radians(col("numTmp"))).show(false)

  df2.withColumn("reverseTemp",reverse(col("arrStr1"))).show(false)

val df3=Seq(("{\"name\":\"tmp\"}","tmp,check,age"),("{\"age\":\"tmp\"}","4,5,age"),
  ("{\"power\":\"200\",\"speed\":\"high\"}","cool,fool,tool")).toDF("tmpJson","tmpCSV")

  df3.withColumn("jsonTemp",schema_of_json(col("tmpJson").cast(StringType))).show(false)

  df3.withColumn("jsonTemp",col("tmpJson").cast(StringType)).show(false)


  //col("numTemp").cast(IntegerType)
  // date_add(Column start, Column days)




  //  (col("tmpConstant")+ lit("2"))

  // df.withColumn("dateTmp",date_format(col("dateTmp"),"YYYY-MM-DD")).show(false)


}
}
