
package org.controller.readFromKafkaAndMaintainAggregate

// import com.typesafe.scalalogging.LazyLogging
import org.util.SparkOpener
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row}
import java.sql.Timestamp
import java.text.SimpleDateFormat
import scala.util.{Success,Try,Failure}
import java.sql.DriverManager
import java.util.Properties

object dailyAggregate  extends SparkOpener /*with LazyLogging */{
  def stringManipulator(s: String) =s.toLowerCase match {
      case value if value.contains("date") => s"cast (${s} as date) ${s}"
      case value if value.contains("timestamp") => s"cast (${s} as timestamp) ${s}"
      case _ => s"cast (${s} as string ) ${s}"
    }
  val formatOfString= new SimpleDateFormat("yyyy-MM-dd hh:mm:ss.SSS")

  def timeStampConverter(x:String)=  Try{formatOfString.parse(x)} match {
      case Success(x) => new  Timestamp(x.getTime)
      case Failure(x) => null
    }

  val spark=SparkSessionLoc()
  spark.sparkContext.setLogLevel("ERROR")
  spark.udf.register("get_timestamp",timeStampConverter(_:String))
  val inputMap=collection.mutable.Map[String,String]()
  import spark.implicits._

  val structForMessage=new StructType(Array(StructField("microService",StringType,true)
    ,StructField("page",StringType,true),StructField("eventDate",StringType,true),StructField("receivedTimestamp",StringType,true)))

//Seq(("","",new java.sql.Date(System.currentTimeMillis),new java.sql.Timestamp(System.currentTimeMillis),0.0)).toDF("micro_service","page","event_date","received_timestamp","batch_id").write.mode("append").partitionBy("event_date","batch_id").saveAsTable("temp_db.page_click_events_bronze")
  def main(args:Array[String]):Unit ={
    for (arg <- args)
      inputMap.put(arg.split("=",2)(0),arg.split("=",2)(1))
    inputMap foreach println
    val readStreamDF=spark.readStream.format("kafka").option("kafka.bootstrap.servers",inputMap("bootstrapServer")).option("subscribe",inputMap("topic")).option("offsets",inputMap("offset")).load.selectExpr("cast(value as string) as value").select(from_json(col("value"),structForMessage).as("tmpParsed")).select("tmpParsed.*").selectExpr("cast (microService as string ) microService","cast (page as string ) page","cast (eventDate as date) eventDate","get_timestamp(receivedTimestamp) receivedTimestamp")

    readStreamDF.writeStream.format("console").option("checkpointLocation",inputMap("checkpointLocation")).queryName("countComputation").foreachBatch {(df:DataFrame,batchId:Long) => {batchFunction(df,batchId)}}.start

    spark.streams.awaitAnyTermination

  }

  /*
  * Daily or day  wise count, groups every microservice per page per day and updates existing count summed with incoming count. (This is SILVER level )
  * Total  count, groups every microservice per page and updates existing count summed with incoming count. (This is total count of all days GOLD level )
  *
  * {"microService":"login","page":"login","eventDate":"2020-08-01","receivedTimestamp":"2020-08-01 23:45:98.214"}
{"microService":"login","page":"login","eventDate":"2020-08-01","receivedTimestamp":"2020-08-01 23:45:98.234"}
{"microService":"login","page":"register","eventDate":"2020-08-02","receivedTimestamp":"2020-08-01 23:45:98.234"}
{"microService":"login","page":"forgot password","eventDate":"2020-08-01","receivedTimestamp":"2020-08-01 23:45:98.234"}
{"microService":"login","page":"login","eventDate":"2020-08-02","receivedTimestamp":"2020-08-01 23:45:98.234"}
{"microService":"login","page":"register","eventDate":"2020-08-01","receivedTimestamp":"2020-08-01 23:45:98.234"}
*
* spark-submit --class org.controller.readFromKafkaAndMaintainAggregate.dailyAggregate --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0,mysql:mysql-connector-java:8.0.25 --conf spark.sql.warehouse.dir=/user/raptor/tmp/hive/warehouse2/ --num-executors 2 --executor-cores 2 --driver-memory 1g --driver-cores 2 --executor-memory 1g --conf spark.sql.shuffle.partitions=4 /home/raptor/IdeaProjects/SparkLearning/build/libs/SparkLearning-1.0-SNAPSHOT.jar  bootstrapServer="localhost:9091,localhost:9092,localhost:9093" topic="tmp.cool.data" offset="latest" checkpointLocation="hdfs://localhost:8020/user/raptor/checkpointLocation/tmpcheck/" jdbcURL="jdbc:mysql://localhost:3306/testPersist?user=raptor&password=" password= user=raptor driver="com.mysql.jdbc.Driver" schemaName=testPersist totalTableName=total_count dailyTableName=day_wise_count hiveSchema=temp_db statsTable=page_click_events_bronze totalColumns="micro_service,page,hit_count" dailyColumns="micro_service,page,event_date,hit_count"
  *  */

  def batchFunction(df:DataFrame,batchId:Long)={
       // df.withColumn("batchID",lit(batchId)).show(false)

        val rowPruning=df.select("microService","page","eventDate").collect
        val totalList=rowPruning.map(x => (x.getString(0),x.getString(1)))
        val dailyList=rowPruning.map(x => (x.getString(0),x.getString(1),x.get(2).toString))

        println(s"dailyList collected = ${dailyList.deep}")
        println(s"totalList collected = ${totalList.deep}")

       val selectQueryForTotal=s"select ${inputMap("totalColumns")} from ${inputMap("schemaName")}.${inputMap("totalTableName")} where micro_service in (${whereConditionGeneratorUpdated(totalList.map(_._1).distinct)}) and page in (${whereConditionGeneratorUpdated(totalList.map(_._2).distinct)}) "
       val selectQueryForDaily=s"select ${inputMap("dailyColumns")} from ${inputMap("schemaName")}.${inputMap("dailyTableName")} where micro_service in (${whereConditionGeneratorUpdated(dailyList.map(_._1).distinct)}) and page in (${whereConditionGeneratorUpdated(dailyList.map(_._2).distinct)}) and event_date in (${whereConditionGeneratorDateUpdated(dailyList.map(_._3.toString).distinct)}) "

        println(s"Total select query on batch ${batchId} ${selectQueryForTotal}")
        println(s"Daily select query on batch ${batchId} ${selectQueryForDaily}")
    // reading required records
        val totalDF=spark.read.format("jdbc")
          .option("url",inputMap("jdbcURL"))
          .option("password",inputMap("password"))
          .option("user",inputMap("user"))
          .option("driver",inputMap("driver"))
          .option("dbtable",s"(${selectQueryForTotal})s")
          .load //spark.sql("select")

       // totalDF.withColumn("totalFromTablePruned",lit("totalFromTablePruned")).show(false)

        val dailyDF=spark.read.format("jdbc")
          .option("url",inputMap("jdbcURL"))
          .option("password",inputMap("password"))
          .option("driver",inputMap("driver"))
          .option("user",inputMap("user"))
          .option("dbtable",s"(${selectQueryForDaily})s")
          .load

       // dailyDF.withColumn("dailyFromTablePruned",lit("dailyFromTablePruned")).show(false)

        /*
         create table testPersist.day_wise_count(
         micro_service varchar(50),
         page varchar(50),
         event_date date,
         last_updated_timestamp timestamp);

         alter table testPersist.day_wise_count add column hit_count long ;

         create table testPersist.total_count(
         micro_service varchar(50),
         page varchar(50),
         last_updated_timestamp timestamp);

         alter table testPersist.total_count add column hit_count long ;


         hive
         CREATE TABLE `temp_db`.`page_click_events_bronze` (
  `micro_service` STRING,
  `page` STRING,
  `received_timestamp` TIMESTAMP,
  `event_date` DATE,
  `batch_id` DOUBLE)
USING parquet
PARTITIONED BY (event_date, batch_id)
LOCATION 'hdfs://localhost:8020/user/raptor/tmp/table/page_click_events_bronze'

*/

        val aggregatedDailyDF= df.groupBy("microService","page","eventDate").agg(count("*").cast(LongType).as("currentBatchCount"))
        val aggregatedTotalDF= df.groupBy("microService","page").agg(count("*").cast(LongType).as("currentBatchCount"))

    spark.sparkContext.broadcast(aggregatedDailyDF)
    spark.sparkContext.broadcast(aggregatedTotalDF)

        val totalTableJoinedRecords=aggregatedTotalDF.as("batchAggregated")
          .join(totalDF.as("totalData"),col("batchAggregated.microService")=== col ("totalData.micro_service") && col("batchAggregated.page")=== col ("totalData.page"),"left")

        val totalUpdationRecords= totalTableJoinedRecords.where("totalData.hit_count is not null")
          .withColumn("finalHits",col("totalData.hit_count")+col("batchAggregated.currentBatchCount"))
          .selectExpr("totalData.micro_service","totalData.page","cast(finalHits as long) as hits")

        val totalInsertionRecords= totalTableJoinedRecords.where("totalData.hit_count is null")
          .selectExpr("batchAggregated.*")
          .selectExpr("microService as micro_service","page as page","currentBatchCount as hit_count")
          .withColumn("last_updated_timestamp",lit(current_timestamp))

      //  totalTableJoinedRecords.withColumn("totalTableJoinedRecords",lit("totalTableJoinedRecords")).show(false)
      //  totalUpdationRecords.withColumn("totalUpdationRecords",lit("totalUpdationRecords")).show(false)
      //  totalInsertionRecords.withColumn("totalInsertionRecords",lit("totalInsertionRecords")).show(false)

        val dailyTableJoinedRecords=aggregatedDailyDF.as("batchAggregated").join(dailyDF.as("dailyData"),
          col("batchAggregated.microService")=== col ("dailyData.micro_service") && col("batchAggregated.page")=== col ("dailyData.page") && col("batchAggregated.eventDate")=== col ("dailyData.event_date")
          ,"left")

        val dailyUpdationRecords= dailyTableJoinedRecords.where("dailyData.hit_count is not null")
          .withColumn("finalHits",lit(col("dailyData.hit_count")+col("batchAggregated.currentBatchCount")).cast(LongType))
          .selectExpr("dailyData.micro_service","dailyData.page","dailyData.event_date","finalHits as hit_count")

        val dailyInsertionRecords= dailyTableJoinedRecords.filter("dailyData.hit_count is null")
          .selectExpr("batchAggregated.*")
          .selectExpr("microService as micro_service","page as page","eventDate as event_date","currentBatchCount as hit_count")
          .withColumn("last_updated_timestamp",lit(current_timestamp))

      //  dailyTableJoinedRecords.withColumn("totalTableJoinedRecords",lit("totalTableJoinedRecords")).show(false)
      //  dailyUpdationRecords.withColumn("totalUpdationRecords",lit("totalUpdationRecords")).show(false)
      //  dailyInsertionRecords.withColumn("totalInsertionRecords",lit("totalInsertionRecords")).show(false)
/*

        totalInsertionRecords
          .write.mode("append").format("jdbc")
          .option("url",inputMap("jdbcURL"))
          .option("password",inputMap("password"))
          .option("user",inputMap("user"))
          .option("driver",inputMap("driver"))
          .option("dbtable",s"${inputMap("schemaName")}.${inputMap("totalTableName")}").save

        dailyInsertionRecords.write.mode("append").format("jdbc")
          .option("url",inputMap("jdbcURL"))
          .option("password",inputMap("password"))
          .option("user",inputMap("user"))
          .option("driver",inputMap("driver"))
          .option("dbtable",s"${inputMap("schemaName")}.${inputMap("dailyTableName")}").save
*/

        // update queries for existing records with action
        dailyUpdationRecords.repartition(col("micro_service"),col("event_date")).map(updateDailyTable(_,inputMap)).toDF.show(false)
        totalUpdationRecords.repartition(col("micro_service")).map(updateTotalTable(_,inputMap)).toDF.show(false)
    // update before insert because the dag for update will be created lazily, so we need to insert whatever is not present and then do an update on existing records
        persistDF(dailyInsertionRecords.repartition(col("micro_service"),col("event_date")),inputMap,inputMap("dailyTableName"))
        persistDF(totalInsertionRecords.repartition(col("micro_service")),inputMap,inputMap("totalTableName"))
        // raw data is persisted to hive
        // spark.sql("show tables in  temp_db").show(false)
        df.selectExpr("microService as micro_service","page","eventDate as event_date","receivedTimestamp as received_timestamp").withColumn("batch_id",lit(batchId)).write.mode("append")/*.partitionBy("event_date","batch_id")*/.insertInto(s"${inputMap("hiveSchema")}.${inputMap("statsTable")}")
  }

  def persistDF(df:org.apache.spark.sql.DataFrame,inputMap:collection.mutable.Map[String,String],tableName:String)=df.write.mode("append").format("jdbc").option("url",inputMap("jdbcURL")).option("password",inputMap("password")).option("user",inputMap("user")).option("driver",inputMap("driver")).option("dbtable",s"${inputMap("schemaName")}.${tableName}").save


  def updateDailyTable(row:Row,inputMap:collection.mutable.Map[String,String])={
    val newHitCount=row.get(3).toString.toLong
    val microService=row.get(0).toString
    val page=row.get(1).toString
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
case class updateDailyRow(hitCount:Long,microService:String,page:String,eventDate:String,numRowsAffected:Int)


  def updateTotalTable(row:Row,inputMap:collection.mutable.Map[String,String])={
    val newHitCount=row.get(2).toString.toLong
    val microService=row.get(0).toString
    val page=row.get(1).toString
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

  def whereConditionGenerator (tmpList:Seq[String]) =tmpList.size match {
    case value if value ==0 => "''"
    case value if value >0 => s"'${tmpList.mkString("','")}'"
  }

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

}


/*
    readStreamDF.writeStream.format("console").option("checkpointLocation",inputMap("checkpointLocation")).foreachBatch {
            (df:DataFrame,batchId:Long) => {
              df.withColumn("batchID",lit(batchId)).show(false)
              // aggregation is written to mysql
    //    var dailyList:Seq[(String,String,String)]=null
    //    var dailyListIndex=0
     //   var totalList:Seq[(String,String)]=null
   //     var totalListIndex=0

        // custom partition pruning
     /*   val dailyCount=df.select("microService","page","eventDate")
          .map(x => (x.get(0).toString,x.get(1).toString,x.get(2).toString))
          .map(x=> {
            dailyListIndex match {
              case value if value == 0 =>
                dailyList = Seq(x)
                dailyListIndex=dailyListIndex+1
              case value if value != 0 =>
                dailyList = dailyList ++ Seq(x)
                dailyListIndex=dailyListIndex+1
            }
            x
            }
        ).count

        val totalCount=df.select("microService","page").map(x => (x.getString(0),x.getString(1)))
          .map(x=>  {
            totalListIndex match {
              case value if value == 0 =>
                totalList = Seq(x)
                totalListIndex+=1
              case value if value != 0 =>
                totalList = totalList ++ Seq(x)
                totalListIndex+=1
          }
            x
          }
          ).count */
      //  logger.info(s"dailyList collected = ${dailyList}")
      //  logger.info(s"totalList collected = ${totalList}")

              val rowPruning=df.select("microService","page","eventDate").collect.toList
              val totalList=rowPruning.toList.map(x => (x.getString(0),x.getString(1)))
              val dailyList=rowPruning.toList.map(x => (x.getString(0),x.getString(1),x.get(2).toString))

              println(s"dailyList collected = ${dailyList}")
              println(s"totalList collected = ${totalList}")


              // reading required records
        val totalDF=spark.read.format("jdbc")
          .option("url",inputMap("jdbcURL"))
          .option("password",inputMap("password"))
          .option("user",inputMap("user"))
          .option("driver",inputMap("driver"))
          .option("dbtable",s"(select ${inputMap("totalColumns")} from ${inputMap("schemaName")}.${inputMap("totalTableName")} where micro_service in (${whereConditionGeneratorUpdated(totalList.map(_._1))}) and page in (${whereConditionGeneratorUpdated(totalList.map(_._2))} )s")
          .load //spark.sql("select")

        totalDF.withColumn("totalFromTablePruned",lit("totalFromTablePruned")).show(false)

        val dailyDF=spark.read.format("jdbc")
          .option("url",inputMap("jdbcURL"))
          .option("password",inputMap("password"))
          .option("driver",inputMap("driver"))
          .option("user",inputMap("user"))
          .option("dbtable",s"(select ${inputMap("dailyColumns")} from ${inputMap("schemaName")}.${inputMap("dailyTableName")} where micro_service in (${whereConditionGeneratorUpdated(dailyList.map(_._1))}) and page in (${whereConditionGeneratorUpdated(dailyList.map(_._2))} and event_date in (${whereConditionGeneratorUpdated(dailyList.map(_._3.toString))}) )s")
          .load

        dailyDF.withColumn("dailyFromTablePruned",lit("dailyFromTablePruned")).show(false)

        /*
         create table testPersist.day_wise_count(
         micro_service varchar(50),
         page varchar(50),
         event_date date,
         last_updated_timestamp timestamp);

         alter table testPersist.day_wise_count add column hit_count long ;

         create table testPersist.total_count(
         micro_service varchar(50),
         page varchar(50),
         last_updated_timestamp timestamp);

         alter table testPersist.total_count add column hit_count long ;
*/

        val aggregatedDF= df.groupBy("microService","page","eventDate").agg(count("*").as("currentBatchCount"))
        spark.sparkContext.broadcast(aggregatedDF)

        val totalTableJoinedRecords=aggregatedDF.as("batchAggregated")
          .join(totalDF.as("totalData"),col("batchAggregated.microService")=== col ("totalData.micro_service") && col("batchAggregated.page")=== col ("totalData.page"),"left")

        val totalUpdationRecords= totalTableJoinedRecords.where("totalData.hits is not null")
          .withColumn("finalHits",lit(col("totalData.hits")+col("batchAggregated.currentBatchCount")))
          .selectExpr("totalData.micro_service","totalData.page","finalHits as hits")

        val totalInsertionRecords= totalTableJoinedRecords.where("totalData.hits is null")
          .selectExpr("batchAggregated.*")
          .selectExpr("microService as micro_service","batchAggregated.page as page","batchAggregated.eventDate as event_date")
          .withColumn("last_updated_timestamp",lit(current_timestamp))

        totalTableJoinedRecords.withColumn("totalTableJoinedRecords",lit("totalTableJoinedRecords")).show(false)
        totalUpdationRecords.withColumn("totalUpdationRecords",lit("totalUpdationRecords")).show(false)
        totalInsertionRecords.withColumn("totalInsertionRecords",lit("totalInsertionRecords")).show(false)


        val dailyTableJoinedRecords=aggregatedDF.as("batchAggregated").join(dailyDF.as("dailyData"),
          col("batchAggregated.microService")=== col ("dailyData.micro_service") && col("batchAggregated.page")=== col ("dailyData.page") && col("batchAggregated.eventDate")=== col ("dailyData.event_date")
          ,"left")

        val dailyUpdationRecords= dailyTableJoinedRecords.where("dailyData.hits is not null")
          .withColumn("finalHits",lit(col("dailyData.hits")+col("batchAggregated.currentBatchCount")))
          .selectExpr("dailyData.micro_service","dailyData.page","dailyData.event_date","finalHits as hits")

        val dailyInsertionRecords= dailyTableJoinedRecords
          .where("dailyData.hits is null")
          .selectExpr("batchAggregated.*")
          .selectExpr("microService as micro_service","batchAggregated.page as page","batchAggregated.eventDate as event_date")
          .withColumn("last_updated_timestamp",lit(current_timestamp))

        dailyTableJoinedRecords.withColumn("totalTableJoinedRecords",lit("totalTableJoinedRecords")).show(false)
        dailyUpdationRecords.withColumn("totalUpdationRecords",lit("totalUpdationRecords")).show(false)
        dailyInsertionRecords.withColumn("totalInsertionRecords",lit("totalInsertionRecords")).show(false)

        totalInsertionRecords
          .write.mode("append").format("jdbc")
          .option("url",inputMap("jdbcURL"))
          .option("password",inputMap("password"))
          .option("user",inputMap("user"))
          .option("driver",inputMap("driver"))
          .option("dbtable",s"${inputMap("schemaName")}.${inputMap("totalTableName")}").save

        dailyInsertionRecords.write.mode("append").format("jdbc")
          .option("url",inputMap("jdbcURL"))
          .option("password",inputMap("password"))
          .option("user",inputMap("user"))
          .option("driver",inputMap("driver"))
          .option("dbtable",s"${inputMap("schemaName")}.${inputMap("dailyTableName")}").save

        persistDF(dailyInsertionRecords,inputMap,inputMap("dailyTableName"))
        persistDF(totalInsertionRecords,inputMap,inputMap("totalTableName"))

// update queries for existing records with action
        dailyUpdationRecords.map(updateDailyTable(_,inputMap)).toDF.show(false)
        totalUpdationRecords.map(updateTotalTable(_,inputMap)).toDF.show(false)
        // raw data is persisted to hive
        df.selectExpr("microService as micro_service","page","eventDate as event_date").withColumn("batch_id",lit(batchId)).write.mode("append").partitionBy("event_date","batch_id").insertInto(s"${inputMap("hiveSchema")}.${inputMap("statsTable")}")
      }}.start*/