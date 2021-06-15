package org.controller.readFromKafkaAndMaintainAggregate

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.util.SparkOpener
import org.apache.spark.sql.{DataFrame, Row}

import java.sql.{DriverManager, Timestamp}
import java.text.SimpleDateFormat
import java.util.Properties
import scala.util.{Failure, Success, Try}

object mapPartitionsTry extends SparkOpener{
  val spark=SparkSessionLoc()
  import spark.implicits._
  spark.sparkContext.setLogLevel("ERROR")
  val structForMessage=new StructType(Array(StructField("microService",StringType,true),StructField("page",StringType,true),StructField("eventDate",StringType,true),StructField("receivedTimestamp",StringType,true)))
  val formatOfString= new SimpleDateFormat("yyyy-MM-dd hh:mm:ss.SSS")
  def timeStampConverter(x:String)=  Try{formatOfString.parse(x)} match {
    case Success(x) => new  Timestamp(x.getTime)
    case Failure(x) => null
  }

  spark.udf.register("get_timestamp",timeStampConverter(_:String))

  def main(args:Array[String]):Unit ={
    val inputMap=collection.mutable.Map[String,String]()
    for (arg <- args)
      inputMap.put(arg.split("=",2)(0),arg.split("=",2)(1))

    val readStreamDF=spark.readStream.format("kafka").option("kafka.bootstrap.servers",inputMap("bootstrapServer")).option("subscribe",inputMap("topic")).option("offsets",inputMap("offset")).load.selectExpr("cast (value as string) value").select(from_json(col("value"),structForMessage).as("tmpParsed")).select("tmpParsed.*").selectExpr("cast (microService as string ) microService","cast (page as string ) page","cast (eventDate as date) eventDate","get_timestamp(receivedTimestamp) receivedTimestamp")

    readStreamDF.writeStream.format("console").foreachBatch((df:DataFrame,batchId:Long)=> {batchFun(df,batchId,inputMap)})
    .option("checkpointLocation",inputMap("checkPoint")).queryName("mapPartitionsTrigger").start

    spark.streams.awaitAnyTermination

  }

  def batchFun(df:DataFrame,batchId:Long,inputMap:collection.mutable.Map[String,String])={
  //  df.withColumn("batchId",lit(batchId)).show(false)
    val dataCollected=df.select("microService","page","eventDate").collect
    val dailyMeta=dataCollected.map(x=>(x.getString(0),x.getString(1),x.get(2).toString))
    val totalMeta=dataCollected.map(x=>(x.getString(0),x.getString(1)))

    val selectQueryForTotal=s"select ${inputMap("totalColumns")} from ${inputMap("schemaName")}.${inputMap("totalTableName")} where micro_service in (${whereConditionGeneratorUpdated(totalMeta.map(_._1).distinct)}) and page in (${whereConditionGeneratorUpdated(totalMeta.map(_._2).distinct)})"
    val selectQueryForDaily=s"select ${inputMap("dailyColumns")} from ${inputMap("schemaName")}.${inputMap("dailyTableName")} where micro_service in (${whereConditionGeneratorUpdated(dailyMeta.map(_._1).distinct)}) and page in (${whereConditionGeneratorUpdated(dailyMeta.map(_._2).distinct)}) and event_date in (${whereConditionGeneratorDateUpdated(dailyMeta.map(_._3).distinct)})"

    val totalDF=spark.read.format("jdbc").option("url",inputMap("jdbcURL"))
      .option("password",inputMap("password"))
      .option("user",inputMap("user"))
      .option("driver",inputMap("driver"))
      .option("dbtable",s"(${selectQueryForTotal})s")
      .load
   // totalDF.show(false)

    val dailyDF=spark.read.format("jdbc").option("url",inputMap("jdbcURL"))
      .option("password",inputMap("password"))
      .option("user",inputMap("user"))
      .option("driver",inputMap("driver"))
      .option("dbtable",s"(${selectQueryForDaily})s")
      .load

   // dailyDF.show(false)

    val dayWiseAgg=df.groupBy("microService","page","eventDate").agg(count("*").as("batchAggregatedCount"))
    val totalAgg=df.groupBy("microService","page").agg(count("*").as("batchAggregatedCount"))
  //  dayWiseAgg.show(false)
  //  totalAgg.show(false)

    val dayWiseJoinDF=dayWiseAgg.as("batch").join(dailyDF.as("fromDB"),
      col("fromDB.micro_service")===col("batch.microService")
      && col("fromDB.page")===col("batch.page")
      && col("fromDB.event_date")===col("batch.eventDate"),"left")

    val totalJoinDF=totalAgg.as("batch").join(totalDF.as("fromDB"),
      col("fromDB.micro_service")===col("batch.microService")
        && col("fromDB.page")===col("batch.page"),"left")

   // dayWiseJoinDF.show(false)
   // totalJoinDF.show(false)

    val dayWiseUpdationRecords=dayWiseJoinDF.filter("fromDB.micro_service is not null")
      .withColumn("computedCount",lit(col("fromDB.hit_count") + col("batch.batchAggregatedCount")))
      .selectExpr("batch.page as page","batch.microService as micro_service","batch.eventDate as event_date","cast(computedCount as long) as hit_count")

    val dayWiseInsertionRecords=dayWiseJoinDF.filter("fromDB.micro_service is null").selectExpr("batch.*").withColumn("receivedTimestamp",lit(current_timestamp))
      .selectExpr("page as page","microService as micro_service","eventDate as event_date","receivedTimestamp as last_updated_timestamp","cast(batchAggregatedCount as long) as hit_count")

    val totalJoinDFUpdationRecords=totalJoinDF.filter("fromDB.micro_service is not null")
      .withColumn("computedCount",lit(col("fromDB.hit_count") + col("batch.batchAggregatedCount")))
      .selectExpr("batch.page as page","batch.microService as micro_service","cast(computedCount as long) as hit_count")

    val totalJoinDFInsertionRecords=totalJoinDF.filter("fromDB.micro_service is null").selectExpr("batch.*").withColumn("receivedTimestamp",lit(current_timestamp))
      .selectExpr("page as page","microService as micro_service","receivedTimestamp as last_updated_timestamp","cast(batchAggregatedCount as long) as hit_count")

 //   dayWiseUpdationRecords.show(false)
 //   dayWiseInsertionRecords.show(false)
 //   totalJoinDFUpdationRecords.show(false)
 //   totalJoinDFInsertionRecords.show(false)

  //  totalJoinDFUpdationRecords.map(updateTotalTable(_,inputMap)).show(false)
  //  dayWiseUpdationRecords.map(updateDailyTable(_,inputMap)).show(false)
   // persistDF(totalJoinDFInsertionRecords,inputMap,inputMap("totalTableName"))
  //  persistDF(dayWiseInsertionRecords,inputMap,inputMap("dailyTableName"))

    // processes the partitions parallely and opens one connection per record in the partition.

    totalJoinDFUpdationRecords.withColumn("totalUpdate",lit("totalUpdate")).repartition(col("page"),col("micro_service")).mapPartitions(x => x.map(updateTotalTable(_,inputMap))).show(false)
    dayWiseUpdationRecords.withColumn("dayWiseUpdate",lit("dayWiseUpdate")).repartition(col("page"),col("micro_service"),col("event_date")).mapPartitions(x => x.map(updateDailyTable(_,inputMap))).show(false)
    totalJoinDFInsertionRecords.withColumn("totalInsert",lit("totalInsert")).repartition(col("page"),col("micro_service")).mapPartitions(x => x.map(insertTotalTable(_,inputMap))).show(false)
    dayWiseInsertionRecords.withColumn("dayWiseInsert",lit("dayWiseInsert")).repartition(col("page"),col("micro_service"),col("event_date")).mapPartitions(x => x.map(insertDailyTable(_,inputMap))).show(false)

    df.selectExpr("microService as micro_service","page","eventDate as event_date","receivedTimestamp as received_timestamp").withColumn("batch_id",lit(batchId.toLong)).write.mode("append").insertInto(s"${inputMap("hiveSchema")}.${inputMap("statsTable")}")

  }
  def persistDF(df:DataFrame,inputMap:collection.mutable.Map[String,String],tableName:String)=df.write.mode("append")
    .option("driver",inputMap("driver"))
    .option("user",inputMap("user"))
    .option("password",inputMap("password"))
    .option("url",inputMap("jdbcURL"))
    .option("dbtable",s"${inputMap("schemaName")}.${tableName}")
    .save


  def insertDailyTable(row:Row,inputMap:collection.mutable.Map[String,String])={
    val hitCount=row.get(4).toString.toLong
    val microService=row.get(1).toString
    val page=row.get(0).toString
    val eventDate=row.get(2).toString
    val lastUpdTimeStamp=row.get(3).toString // this is the received timeStamp While insertion
    val insertStatement=s"insert into ${inputMap("schemaName")}.${inputMap("dailyTableName")} (page,micro_service,event_date,hit_count,last_updated_timestamp) values ('${page}','${microService}','${eventDate}','${hitCount}','${lastUpdTimeStamp}')"
    println(s"insert statement daily table ${insertStatement}")
    Class.forName(inputMap("driver"))
    val props=new Properties
    props.put("user",inputMap("user"))
    props.put("password",inputMap("password"))
    props.put("url",inputMap("jdbcURL"))
    val connection=DriverManager.getConnection(props.getProperty("url"),props)
    val preparedStatement=connection.prepareStatement(insertStatement)
    val numRowsAffected=preparedStatement.executeUpdate
    preparedStatement.close
    connection.close
    updateDailyRow(hitCount,microService,page,eventDate,numRowsAffected)
  }
  case class updateDailyRow(hitCount:Long,microService:String,page:String,eventDate:String,numRowsAffected:Int)

  def updateDailyTable(row:Row,inputMap:collection.mutable.Map[String,String])={
    val newHitCount=row.get(3).toString.toLong
    val microService=row.get(1).toString
    val page=row.get(0).toString
    val eventDate=row.get(2).toString
    val updateStatement=s"update ${inputMap("schemaName")}.${inputMap("dailyTableName")} set last_updated_timestamp=now(),hit_count='${newHitCount}' where micro_service ='${microService}' and page='${page}' and event_date='${eventDate}'"
    // logger.info(s"update statement daily table ${updateStatement}")
    println(s"update statement daily table ${updateStatement}")
    Class.forName(inputMap("driver"))
    val props=new Properties
    props.put("user",inputMap("user"))
    props.put("password",inputMap("password"))
    props.put("url",inputMap("jdbcURL"))
    val connection=DriverManager.getConnection(props.getProperty("url"),props)
    val preparedStatement=connection.prepareStatement(updateStatement)
    val numRowsAffected=preparedStatement.executeUpdate
    preparedStatement.close
    connection.close
    updateDailyRow(newHitCount,microService,page,eventDate,numRowsAffected)
  }

  def insertTotalTable(row:Row,inputMap:collection.mutable.Map[String,String])={
    val hitCount=row.get(3).toString.toLong
    val microService=row.get(1).toString
    val page=row.get(0).toString
    val lastUpdTimeStamp=row.get(2).toString
    val insertStatement=s"insert into ${inputMap("schemaName")}.${inputMap("totalTableName")} (page,micro_service,hit_count,last_updated_timestamp) values('${page}','${microService}','${hitCount}','${lastUpdTimeStamp}') "
    // logger.info(s"update statement total table ${updateStatement}")
    println(s"insert statement total table ${insertStatement}")
    Class.forName(inputMap("driver"))
    val props=new Properties
    props.put("user",inputMap("user"))
    props.put("password",inputMap("password"))
    props.put("url",inputMap("jdbcURL"))
    val connection=DriverManager.getConnection(props.getProperty("url"),props)
    val preparedStatement=connection.prepareStatement(insertStatement)
    val numRowsAffected=preparedStatement.executeUpdate
    preparedStatement.close
    connection.close
    updateTotalRow(hitCount,microService,page,numRowsAffected)
  }

  def updateTotalTable(row:Row,inputMap:collection.mutable.Map[String,String])={
    val newHitCount=row.get(2).toString.toLong
    val microService=row.get(1).toString
    val page=row.get(0).toString
    val updateStatement=s"update ${inputMap("schemaName")}.${inputMap("totalTableName")} set last_updated_timestamp=now(),hit_count='${newHitCount}' where micro_service ='${microService}' and page='${page}'"
    // logger.info(s"update statement total table ${updateStatement}")
    println(s"update statement total table ${updateStatement}")
    Class.forName(inputMap("driver"))
    val props=new Properties
    props.put("user",inputMap("user"))
    props.put("password",inputMap("password"))
    props.put("url",inputMap("jdbcURL"))
    val connection=DriverManager.getConnection(props.getProperty("url"),props)
    val preparedStatement=connection.prepareStatement(updateStatement)
    val numRowsAffected=preparedStatement.executeUpdate
    preparedStatement.close
    connection.close
    updateTotalRow(newHitCount,microService,page,numRowsAffected)
  }
 case class updateTotalRow(hitCount:Long,microService:String,page:String,numRowsAffected:Int)


  def whereConditionGeneratorUpdated (tmpList:Seq[String]) = {
    var tmpString=s""
    var numIndex=1
    tmpList.size match {
      case value if value == 0 => tmpString="''"
      case value if value > 0 => for (tmpStr <- tmpList)
        numIndex match {
          case value if value == 1 =>
            tmpString+=s"'${tmpStr}'"
            numIndex+=1
          case value if value > 1 =>
            tmpString+=s",'${tmpStr}'"
            numIndex+=1
        }
    }
    tmpString
  }

  def whereConditionGeneratorDateUpdated (tmpList:Seq[String]) = {
    var tmpString=s""
    var numIndex=1
    tmpList.size match {
      case value if value == 0 => tmpString="null"
      case value if value > 0 => for (tmpStr <- tmpList)
        numIndex match {
          case value if value == 1 =>
            tmpString+=s"'${tmpStr}'"
            numIndex+=1
          case value if value > 1 =>
            tmpString+=s",'${tmpStr}'"
            numIndex+=1
        }
    }
    tmpString
  }

    /*

    same functionality as daily aggregate but uses map partitions and scala code

  spark-submit --class org.controller.readFromKafkaAndMaintainAggregate.mapPartitionsTry --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0,mysql:mysql-connector-java:8.0.25 --conf spark.sql.warehouse.dir=/user/raptor/tmp/hive/warehouse2/ --num-executors 2 --executor-cores 2 --driver-memory 1g --driver-cores 2 --executor-memory 1g --conf spark.sql.shuffle.partitions=4 /home/raptor/IdeaProjects/SparkLearning/build/libs/SparkLearning-1.0-SNAPSHOT.jar  bootstrapServer="localhost:9091,localhost:9092,localhost:9093" topic="tmp.cool.data" offset="latest" checkPoint="hdfs://localhost:8020/user/raptor/checkpointLocation/tmpcheck/" jdbcURL="jdbc:mysql://localhost:3306/testPersist?user=raptor&password=" password= user=raptor driver="com.mysql.jdbc.Driver" schemaName=testPersist totalTableName=total_count dailyTableName=day_wise_count hiveSchema=temp_db hiveBronzeTable=page_click_events_bronze totalColumns="micro_service,page,hit_count" dailyColumns="micro_service,page,event_date,hit_count" hiveStatsTable=batch_micro_service_stats statsTable=page_click_events_bronze

{"microService":"login","page":"login","eventDate":"2020-08-01","receivedTimestamp":"2020-08-01 23:45:98.214"}
{"microService":"login","page":"login","eventDate":"2020-08-01","receivedTimestamp":"2020-08-01 23:45:98.234"}
{"microService":"login","page":"register","eventDate":"2020-08-02","receivedTimestamp":"2020-08-01 23:45:98.234"}
{"microService":"login","page":"forgot password","eventDate":"2020-08-01","receivedTimestamp":"2020-08-01 23:45:98.234"}
{"microService":"login","page":"login","eventDate":"2020-08-02","receivedTimestamp":"2020-08-01 23:45:98.234"}
{"microService":"login","page":"register","eventDate":"2020-08-01","receivedTimestamp":"2020-08-01 23:45:98.234"}
{"microService":"order","page":"add-to-cart","eventDate":"2020-08-01","receivedTimestamp":"2020-08-01 23:45:98.234"}
{"microService":"order","page":"remove-from-cart","eventDate":"2020-08-01","receivedTimestamp":"2020-08-01 23:45:98.234"}
{"microService":"order","page":"quantity-cart","eventDate":"2020-08-01","receivedTimestamp":"2020-08-01 23:45:98.234"}
{"microService":"order","page":"checkout","eventDate":"2020-08-01","receivedTimestamp":"2020-08-01 23:45:98.234"}
{"microService":"checkout","page":"pay","eventDate":"2020-08-01","receivedTimestamp":"2020-08-01 23:45:98.234"}
{"microService":"checkout","page":"quantity","eventDate":"2020-08-01","receivedTimestamp":"2020-08-01 23:45:98.234"}
{"microService":"pay","page":"option","eventDate":"2020-08-01","receivedTimestamp":"2020-08-01 23:45:98.234"}


{"microService":"order","page":"quantity-cart","eventDate":"2020-08-01","receivedTimestamp":"2020-08-01 23:45:98.234"}
{"microService":"order","page":"checkout","eventDate":"2020-08-01","receivedTimestamp":"2020-08-01 23:45:98.234"}
{"microService":"checkout","page":"pay","eventDate":"2020-08-01","receivedTimestamp":"2020-08-01 23:45:98.234"}
{"microService":"checkout","page":"quantity","eventDate":"2020-08-01","receivedTimestamp":"2020-08-01 23:45:98.234"}
{"microService":"pay","page":"option","eventDate":"2020-08-01","receivedTimestamp":"2020-08-01 23:45:98.234"}
{"microService":"pay","page":"debit","eventDate":"2020-08-01","receivedTimestamp":"2020-08-01 23:45:98.214"}
{"microService":"pay","page":"credit","eventDate":"2020-08-01","receivedTimestamp":"2020-08-01 23:45:98.214"}*/
}
