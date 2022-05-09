package org.controller.markCalculation

import org.apache.spark.sql.streaming.GroupStateTimeout
import org.controller.markCalculation.marksCalculationUtil.{jsonStrToMap, toJson}

object dynamicSchemaSCD2 {

  val spark = org.apache.spark.sql.SparkSession.builder.getOrCreate
  spark.sparkContext.setLogLevel("ERROR")
    val inputMap=collection.mutable.Map[String,String]()

def main(args:Array[String]):Unit = {

  val getDataType: (String) => org.apache.spark.sql.types.DataType = (dTypeInfo: String) => dTypeInfo.toLowerCase match {
    case value if value.startsWith("str") => org.apache.spark.sql.types.StringType
    case value if value.startsWith("int") => org.apache.spark.sql.types.IntegerType
    case value if value.startsWith("double") => org.apache.spark.sql.types.DoubleType
    case value if value.startsWith("char") => org.apache.spark.sql.types.CharType(value.split("\\(").last.split("\\)").head.toInt)
    case value if value.startsWith("float") => org.apache.spark.sql.types.FloatType
    case value if value.startsWith("decimal") =>
      val (precision, scale) = (value.split(",").head.split("\\(").last.toInt, value.split(",").last.split("\\)").head.toInt)
      println(s"precision = ${precision} scale ${precision}")
      org.apache.spark.sql.types.DecimalType(precision, scale)
    case value if value.startsWith("date") => org.apache.spark.sql.types.DateType
    case value if value.startsWith("time") => org.apache.spark.sql.types.TimestampType
  }

  def getStructField(colInfo: String) =
    colInfo.split(":", 2) match {
      case value =>
        org.apache.spark.sql.types.StructField(value(0), getDTypeNew(value(1)), true)
    }

  def getDTypeNew(dTypeString: String): org.apache.spark.sql.types.DataType =
    dTypeString.toLowerCase match {
      case value if value.startsWith("str") => org.apache.spark.sql.types.StringType
      case value if value.startsWith("deci") =>
        val (scale, precision) = value.split("\\(") match {
          case value => (value.last.split(",").head, value.last.split(",").last.split("\\)").head)
        }
        // when precision is greater than scale throw an error and assign 30 % of scale to precision
        scale > precision match {
          case true => org.apache.spark.sql.types.DecimalType(scale.toInt, precision.toInt)
          case false => org.apache.spark.sql.types.DecimalType(scale.toInt, Math.ceil((scale.toInt / 100.0) * 30).toInt)
        }
      case value if value.startsWith("int") => org.apache.spark.sql.types.IntegerType
      case value if value.startsWith("doub") => org.apache.spark.sql.types.DoubleType
      case value if value.startsWith("char") => org.apache.spark.sql.types.CharType(value.split("\\(").last.split(')').head.toInt)
      case value if value.startsWith("long") => org.apache.spark.sql.types.LongType
      case value if value.startsWith("date") => org.apache.spark.sql.types.DateType
      case value if value.startsWith("timestamp") => org.apache.spark.sql.types.TimestampType
    }


  def getKeyFromInnerMessage(innerMsg : String) =
    jsonStrToMap(innerMsg) match {
      case value => (value("semId").asInstanceOf[String],value("examId").toString)
    }

  def getKey(row: org.apache.spark.sql.Row
             , inputMap: scala.collection.mutable.Map[String, String]) =
    getKeyFromInnerMessage(row.getAs[String](inputMap("actualMsgColumn")))

  case class stateStore(dataMap: collection.mutable.Map[String, List[org.apache.spark.sql.Row]])

  type driver = String
  case class ConnectionDetails(user: String, password: String, url: String, driver: driver)

  def getProps = new java.util.Properties

  def getJDBCProps(connectionDetails: ConnectionDetails) = getProps match {
    case value =>
      value.put("url", connectionDetails.url)
      value.put("user", connectionDetails.user)
      value.put("password", connectionDetails.password)
      value
  }

  def getJdbcConnection(connectionDetails: ConnectionDetails) = {
    val props = getJDBCProps(connectionDetails)
    Class.forName(connectionDetails.driver)
    java.sql.DriverManager.getConnection(props.getProperty("user"), props)
  }

  case class connectionHolder(connectionDetails: ConnectionDetails) {
    var connectionVariable: java.sql.Connection = null

    def connection = connectionVariable.isValid(5) match {
      case true => connectionVariable
      case false =>
        connectionVariable = getJdbcConnection(connectionDetails)
        connectionVariable
    }
  }

  def constructConnectionConfig(dbName: String) = ConnectionDetails(inputMap(dbName + "User"), inputMap(dbName + "Password"), inputMap(dbName + "Url"), inputMap(dbName + "Driver"))

  val postgresConnection = connectionHolder(constructConnectionConfig("postgres"))

  val getSemIdFromTable = (semId: String) => postgresConnection.connection.prepareStatement(s"select count(*) as rowCount from ${inputMap("lookupTable")} where sem_id='${semId}' and is_valid=1").executeQuery.next
  val getSemIdAndExamIdFromTable = (semId: String, examId: String) => postgresConnection.connection.prepareStatement(s"select exam_id,sem_id from ${inputMap("lookupTable")} where sem_id='${semId}' exam_id='${examId}' and is_valid=1").executeQuery.next


  def filterRecordsForIOUChild(incomingRow: org.apache.spark.sql.Row, idsToCheck: List[(String, String)]) = {
    def checkID(dataMap: Map[String, Any], filterList: List[(String, String)]) =
      filterList.map(_._1).contains(dataMap("semId").asInstanceOf[String]) && idsToCheck.map(_._2).contains(dataMap("examId").asInstanceOf[String])

    jsonStrToMap(incomingRow.getAs[String](inputMap("actualMsgColumn"))) match {
      case value =>
        value("CRUDType") match {
          case "Insert" =>
            checkID(value, idsToCheck)
          case "Update" =>
            checkID(value, idsToCheck)
          case "Delete" =>
            false
        }
    }
  }
  def filterGrandChildRecords(row:org.apache.spark.sql.Row,deleteKeys:List[String]) =
    jsonStrToMap(row.getAs[String](inputMap("actualMsgColumn"))) match {
      case value =>
        value("CRUDType") match {
          case "Delete" => false
          case "Insert" || "Update" =>
            !deleteKeys.contains(value("examId"))
        }
    }
  val simpleDateParser = new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss.SSS")

  def deleteChildRow(keyCombination:(String,String))=s"update set is_valid=0 where exam_id = '${keyCombination._2}' and sem_id='${keyCombination._1}' and is_valid=1"

  def getLatestRecord(rowTmp:List[org.apache.spark.sql.Row])=rowTmp.sortBy[Long](x=> simpleDateParser.parse(x.getAs[String]("receivingTimeStamp")).getTime).last

  def getLatestDeleteRecord(rowTmp:List[org.apache.spark.sql.Row])=rowTmp.sortWith((one:org.apache.spark.sql.Row,two:org.apache.spark.sql.Row) => {
    simpleDateParser.parse(one.getAs[String]("receivingTimeStamp")).getTime > simpleDateParser.parse(two.getAs[String]("receivingTimeStamp")).getTime
  }).head match {
    case value =>
      jsonStrToMap(value.getAs[String](inputMap("actualMsgColumn")))("CRUDType").asInstanceOf[String]=="Delete" match {
        case true =>
          value
        case false =>
          org.apache.spark.sql.Row(value.schema.map(_.name).foldLeft(Seq.empty[Any])((seqVariable,currVariable)=>
            currVariable==inputMap("actualMsgColumn") match  {
              case true =>
                seqVariable :+ toJson(jsonStrToMap(value.getAs[String](currVariable)).filterNot(_._1 == "CRUDType") ++ Map("CRUDType"->"Delete"))
              case false =>
                seqVariable :+ value.getAs[String](currVariable)
            }) :_*)
      }
  }

  def getLatestRecordWithDeleteCheck(rowTmp:List[org.apache.spark.sql.Row])= getDeleteRecords(rowTmp).size match {
    case 0 => getLatestRecord(rowTmp)
    case _ => getLatestDeleteRecord(rowTmp)
  }

  def getDeleteRecords(rowTmp:List[org.apache.spark.sql.Row])=rowTmp.filter(x => jsonStrToMap(x.getAs[String](inputMap("actualMsgColumn")))("CRUDType").asInstanceOf[String]=="Delete" )
  def replicateGrandChild(record:org.apache.spark.sql.Row)=org.apache.spark.sql.Row(record.schema.foldRight(Seq.empty[Any])((seqVar)))

  /*
          assessment is pnt // soft deletes postgres table // scd2 deletes in delta
          childExamAndSub child  // scd2 deletes in delta
          childExamAndType is grand child // scd2 deletes in delta
        */

  def stateFunction(key: (String,String), incomingRowList: List[org.apache.spark.sql.Row],
                    groupState: org.apache.spark.sql.streaming.GroupState[stateStore]
                   ) = groupState.getOption match {
      case Some(state) =>
      case None =>
        var releasedRows=Seq.empty[org.apache.spark.sql.Row]
        val tmpMap=collection.mutable.Map[String,List[org.apache.spark.sql.Row]]()
        val parentRows = incomingRowList.filter(_.getAs[String](inputMap("typeFilterColumn")) == inputMap("assessmentYearRefValue"))
        val childExamAndSub = incomingRowList.filter(_.getAs[String](inputMap("typeFilterColumn")) == inputMap("semIdExamIdSubCodeRefValue"))
        val grandChildExamAndType = incomingRowList.filter(_.getAs[String](inputMap("typeFilterColumn")) == inputMap("semIdExamIdExamTypeRefValue"))

        parentRows.size match {
          case 0 =>
            childExamAndSub.size match {
              case value if value >= 1 =>
                val childCRUDDeleteCheck=childExamAndSub.filter(x => jsonStrToMap(x.getAs[String](inputMap("actualMsgColumn")))("CRUDType").toString match {case "Insert" || "Update" => false case "Delete"  =>true })
                // delete grandChild too
                childCRUDDeleteCheck.size >=1 match {
                  case true =>
                    val childLatestRecord=getLatestDeleteRecord(childExamAndSub)
                    val grandChildLatestRecord=getLatestDeleteRecord(grandChildExamAndType)
                    getSemIdAndExamIdFromTable(key._1,key._2) match {
                      case true =>
                        releasedRows= releasedRows ++ Seq(
                          childLatestRecord,
                          grandChildLatestRecord
                        )
                      case false =>{println(s"No Release, no parent found in msg and table. No state")}
                    }

                    tmpMap.put("child",List(childLatestRecord))
                    tmpMap.put("grandChild",List(grandChildLatestRecord))

                  case false =>
                    val childLatestRecord= getLatestRecord(childExamAndSub)
                    val grandChildLatestRecord=getLatestRecordWithDeleteCheck(grandChildExamAndType)

                    getSemIdAndExamIdFromTable(key._1,key._2) match {
                      case true =>
                        releasedRows= releasedRows ++ Seq(
                          childLatestRecord,
                          grandChildLatestRecord
                        )
                      case false =>{println(s"No Release, no parent found in msg and table. No state")}
                    }

                    tmpMap.put("child",List(childLatestRecord))
                    tmpMap.put("grandChild",List(grandChildLatestRecord))
                      }
              case 0 =>

                val latestGrandChildRecord=getLatestRecordWithDeleteCheck(grandChildExamAndType)
                getSemIdAndExamIdFromTable(key._1,key._2) match {
                  case true =>
                    releasedRows = releasedRows :+ latestGrandChildRecord
                  case false => {println("No parent in table, grand child not released")}
                 }
                 tmpMap.put("grandChild",List(latestGrandChildRecord))
            }
          case _ =>  // parent row incoming
            getDeleteRecords(parentRows).size match {
              case 0 =>
                (getDeleteRecords(childExamAndSub),childExamAndSub,getDeleteRecords(grandChildExamAndType),getDeleteRecords(grandChildExamAndType)) match {
                  case (delChildRec,inChildRecords,delGrandChildRec,inGrandChildRecords) if delChildRec.size ==0
                    && inChildRecords.size >0  && delGrandChildRec.size==0 && inGrandChildRecords.size>0=>
                  // latest child and grandchild and parent
                    val latestChildRecord=getLatestRecord(childExamAndSub)
                    val latestGrandChildRecord=getLatestRecord(grandChildExamAndType)
                    val latestParentRecord=getLatestRecord(parentRows)

                  case (delChildRec,inChildRecords,delGrandChildRec,inGrandChildRecords) if delChildRec.size >0
                    && inGrandChildRecords.size>0=>
                  //latest parent, delete child and grandchild
                    val latestChildRecord=getLatestDeleteRecord(childExamAndSub)
                    val latestGrandChildRecord=getLatestDeleteRecord(grandChildExamAndType)
                    val latestParentRecord=getLatestRecord(parentRows)



                  case (delChildRec,inChildRecords,delGrandChildRec,inGrandChildRecords) if inChildRecords.size == 0
                    && delGrandChildRec.size ==0 && inGrandChildRecords.size >0=>

                  //latest parent and grandchild , no child

                    val latestGrandChildRecord=getLatestRecord(grandChildExamAndType)
                    val latestParentRecord=getLatestRecord(parentRows)

                  case (delChildRec,inChildRecords,delGrandChildRec,inGrandChildRecords) if inChildRecords.size == 0
                    && delGrandChildRec.size > 0=>
                    //latest parent, delete grandchild , no child

                    val latestGrandChildRecord=getLatestDeleteRecord(grandChildExamAndType)
                    val latestParentRecord=getLatestRecord(parentRows)

                  case (delChildRec,inChildRecords,delGrandChildRec,inGrandChildRecords) if delChildRec.size ==0
                && inChildRecords.size >0  && delGrandChildRec.size >0 =>
                  // latest parent,child . delete grandchild

                    val latestChildRecord=getLatestRecord(childExamAndSub)
                    val latestGrandChildRecord=getLatestDeleteRecord(grandChildExamAndType)
                    val latestParentRecord=getLatestRecord(parentRows)

                  case (delChildRec,inChildRecords,delGrandChildRec,inGrandChildRecords) if inChildRecords.size >0
                    && inGrandChildRecords.size == 0 =>
                  // latest parent,child . no grandchild

                    val latestChildRecord=getLatestRecord(childExamAndSub)
                    val latestParentRecord=getLatestRecord(parentRows)

                  case (delChildRec,inChildRecords,delGrandChildRec,inGrandChildRecords) if inChildRecords.size >0
                    && delChildRec.size > 0 && inGrandChildRecords.size == 0 =>
                  // latest parent,child . replicate grandchild Type withDelete
                    val latestChildRecord=getLatestDeleteRecord(childExamAndSub)
                    val latestParentRecord=getLatestRecord(parentRows)
                    val latestGrandParentRecord=getLatestDeleteRecord(replicateGrandChild(childExamAndSub))


                }
              case _ =>
                getLatestDeleteRecord(parentRows)
                (getLatestRecord(childExamAndSub),getLatestRecord(grandChildExamAndType)) match {
                  case sie==0
                    replicateGrandChild, replicateChild
                }

              //check if child and grand child are present, if yes thenn replicate eith delte


            }


        }
    }




  // colName:Dtype()~colName:Dtype~colName:Dtype
/*
posgres table
set schema 'temp_schema';

create table temp_db.temp_schema.sem_id_and_exam_id (
sem_id varchar(20),
exam_id varchar(20),
is_valid  boolean
);

*/

  def getSchema(schemaInfo:String) =new org.apache.spark.sql.types.StructType(
    schemaInfo.split("~").map(getStructField))


  for(arg <- args)
    inputMap.put(arg.split("=",2)(0),arg.split("=",2)(1))

  val readStreamDF=spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers",inputMap("bootstrapServer"))
    .option("startingOffsets",inputMap("startingOffsets"))
    .option("subscribe",inputMap("topic")).load.select(
    org.apache.spark.sql.functions.from_json(org.apache.spark.sql.functions.col("value")
      .cast(org.apache.spark.sql.types.StringType),getSchema(inputMap("outerSchema"))).as("dataExtracted"))
    .select(org.apache.spark.sql.functions.col("dataExtracted.*"))
    .groupByKey(getKey(_,inputMap))(
      org.apache.spark.sql.catalyst.encoders.RowEncoder(Array()
      )).mapGroupsWithState(GroupStateTimeout.NoTimeout)(
    stateFunction

   )(org.apache.spark.sql.catalyst.encoders.RowEncoder(new org.apache.spark.sql.types.StructType(
    Array(

    )
  )))
           /*
          typeFilterColumn
           assessmentYearRefValue
          semIdExamIdSubCodeRefValue
          semIdExamIdExamTypeRefValue

          actualMsgColumn
           assessmentYearSchema


            */
        readStreamDF
      .filter(s"${inputMap("typeFilterColumn")} = '${inputMap("assessmentYearRefValue")}'")
      .select(org.apache.spark.sql.functions.from_json
      (org.apache.spark.sql.functions.col(inputMap("actualMsgColumn"))
      ,getSchema(inputMap("assessmentYearSchema"))))
         .writeStream.format("console").outputMode("append")
         .option("checkpointLocation","")
         .start


    val semIdExamIdSubjectCodeDF= readStreamDF
      .filter(org.apache.spark.sql.functions.col(inputMap("typeFilterColumn"))
        === org.apache.spark.sql.functions.lit(inputMap("semIdExamIdSubCodeRefValue")))

    val semIdExamIdAndExamTypeDF= readStreamDF
      .where(org.apache.spark.sql.functions.col(inputMap("typeFilterColumn"))
        === org.apache.spark.sql.functions.lit(inputMap("semIdExamIdExamTypeRefValue")))


  /*
  spark.readStream.format("kafka").option("kafka.bootstrap.servers",inputMap("bootstrapServer"))
    .option("startingOffsets",inputMap("startingOffsets"))
    .option("subscribe",inputMap("topic")).load.select(org.apache.spark.sql.functions.from_json(org.apache.spark.sql.functions.col("value")
  .cast(org.apache.spark.sql.types.StringType).as("value"),getSchema(inputMap("schemaStr"))))
    .writeStream.format("console").outputMode("append").option("truncate","false")
    .option("numRows","999999999")
    .option("checkpointLocation",inputMap("checkpointLocation")).start

*/


  /*


  semId to examId mapping

  examId to subjectCode mapping (with date)

  examId to examType

  semId to assessmentYear

  bootstrapServer="localhost:8081,localhost:8082,localhost:8083"
  startingOffsets=latest
  outerSchema=
  topic=


  topic1="topic.one"
  topic2="topic.two"
  checkpointLocation1="hdfs://localhost:8020/user/raptor/stream/checkpoint/schema1/"
  checkpointLocation2="hdfs://localhost:8020/user/raptor/stream/checkpoint/schema2/"
  schemaStr1="semId:str,examId:string,incomingDate:Date"
  schemaStr2="examId:str,subjectCode:string,examDate:date,incomingDate:date"

  spark-submit --class org.controller.markCalculation.dynamicSchemaSCD2 --num-executors 2 --executor-cores 2 --driver-cores 2 --executor-memory 512m --driver-memory 512m --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0,io.delta:delta-core_2.12:0.8.0 /home/raptor/IdeaProjects/SparkLearning/build/libs/SparkLearning-1.0-SNAPSHOT.jar bootstrapServer="localhost:8081,localhost:8082,localhost:8083" startingOffsets=latest topic1="topic.one" topic2="topic.two" checkpointLocation1="hdfs://localhost:8020/user/raptor/stream/checkpoint/schema1/" checkpointLocation2="hdfs://localhost:8020/user/raptor/stream/checkpoint/schema2/" schemaStr1="semId:str~examId:string~incomingDate:Date" schemaStr2="examId:str~subjectCode:string~examDate:date~incomingDate:date"

{"semId":"sem001","examId":"e001","incomingDate":"2020-09-10"}


   */
/*
  spark.readStream.format("kafka").option("kafka.bootstrap.servers",inputMap("bootstrapServer"))
    .option("startingOffsets",inputMap("startingOffsets"))
    .option("subscribe",inputMap("topic1")).load
    .select(org.apache.spark.sql.functions.from_json(org.apache.spark.sql.functions.col("value")
    .cast(org.apache.spark.sql.types.StringType),getSchema(inputMap("schemaStr1"))).as("tmpVal"))
    .select(org.apache.spark.sql.functions.col("tmpVal.*"))
    .writeStream.format("console").outputMode("append").option("truncate","false")
    .option("numRows","999999999")
    .option("checkpointLocation",inputMap("checkpointLocation1")).start

  spark.readStream.format("kafka").option("kafka.bootstrap.servers",inputMap("bootstrapServer"))
    .option("startingOffsets",inputMap("startingOffsets"))
    .option("subscribe",inputMap("topic2")).load.withColumn("tmpVal",org.apache.spark.sql.functions.from_json(org.apache.spark.sql.functions.col("value")
    .cast(org.apache.spark.sql.types.StringType),getSchema(inputMap("schemaStr2"))))
    .select(org.apache.spark.sql.functions.col("tmpVal.*"))
    .writeStream.format("console").outputMode("append").option("truncate","false")
    .option("numRows","999999999")
    .option("checkpointLocation",inputMap("checkpointLocation2")).start
*/
  spark.streams.awaitAnyTermination







 }
}
