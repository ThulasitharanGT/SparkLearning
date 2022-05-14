package org.controller.markCalculation

import org.apache.spark.sql.catalyst.encoders.{ExpressionEncoder, RowEncoder}
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.streaming.GroupStateTimeout
import org.controller.markCalculation.marksCalculationConstant.wrapperSchema
import org.controller.markCalculation.marksCalculationUtil.{jsonStrToMap, toJson}
import org.apache.spark.sql.{Encoder, Encoders}

object dynamicSchemaSCD2 {

  case class rowStore(messageType:String,actualMessage:String,receivingTimeStamp:String)
  case class stateStore(dataMap: collection.mutable.Map[String, List[rowStore]])



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
    println(s"connectionDetails ${connectionDetails}" )
    Class.forName(connectionDetails.driver)
    java.sql.DriverManager.getConnection(props.getProperty("url"), props)
  }

  case class connectionHolder(connectionDetails: ConnectionDetails) {
    var connectionVariable: java.sql.Connection =null
    var assignJDBCConnection = () => connectionVariable = getJdbcConnection(connectionDetails)

    def connection = scala.util.Try{connectionVariable.isValid(5)} match {
      case scala.util.Success(true) =>
        println(s"connection exits Already ${connectionVariable}")
        connectionVariable
      case  scala.util.Success(false) =>
        println(s"connection closed opening new connection")
        assignJDBCConnection()
        println(s"Opened new connection ${connectionVariable}")
        connectionVariable
      case scala.util.Failure(f) =>
        println(s"Opening connection for the first time")
        assignJDBCConnection()
        println(s"Opened new connection ${connectionVariable}")
        connectionVariable
    }

  }

  def constructConnectionConfig(dbName: String,inputMap:collection.mutable.Map[String,String]) = ConnectionDetails(inputMap(dbName + "User"), inputMap(dbName + "Password"), inputMap(dbName + "Url"), inputMap(dbName + "Driver"))

  var postgresConnection:connectionHolder =null

  val getSemIdFromTable = (semId: String,inputMap:collection.mutable.Map[String,String]) => postgresConnection.connection.prepareStatement(s"select count(*) as rowCount from ${inputMap("lookupTable")} where sem_id='${semId}' and is_valid=true").executeQuery.next
  val getSemIdAndExamIdFromTable = (semId: String, examId: String,inputMap:collection.mutable.Map[String,String]) => postgresConnection.connection.prepareStatement(s"select exam_id,sem_id from ${inputMap("lookupTable")} where sem_id='${semId}' and exam_id='${examId}' and is_valid=true").executeQuery.next


  def filterRecordsForIOUChild(incomingRow: org.apache.spark.sql.Row, idsToCheck: List[(String, String)],inputMap:collection.mutable.Map[String,String]) = {
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
  def filterGrandChildRecords(row:org.apache.spark.sql.Row,deleteKeys:List[String],inputMap:collection.mutable.Map[String,String]) =
    jsonStrToMap(row.getAs[String](inputMap("actualMsgColumn"))) match {
      case value =>
        value("CRUDType") match {
          case "Delete" => false
          case tmp if tmp =="Insert" || tmp == "Update" =>
            !deleteKeys.contains(value("examId"))
        }
    }
  val simpleDateParser = new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss.SSS")

  def deleteChildRow(keyCombination:(String,String),inputMap:collection.mutable.Map[String,String])=s"update ${inputMap("postgresTableName")} set is_valid=false where exam_id = '${keyCombination._2}' and sem_id='${keyCombination._1}' and is_valid=true"

  def getLatestRecord(rowTmp:List[org.apache.spark.sql.Row])=rowTmp.sortBy[Long](x=> simpleDateParser.parse(x.getAs[String]("receivingTimeStamp")).getTime).last

  def getLatestDeleteRecord(rowTmp:List[org.apache.spark.sql.Row],inputMap:collection.mutable.Map[String,String])=rowTmp.sortWith((one:org.apache.spark.sql.Row,two:org.apache.spark.sql.Row) => {
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

  def getLatestRecordWithDeleteCheck(rowTmp:List[org.apache.spark.sql.Row],inputMap:collection.mutable.Map[String,String])= getDeleteRecords(rowTmp,inputMap).size match {
    case 0 => getLatestRecord(rowTmp)
    case _ => getLatestDeleteRecord(rowTmp,inputMap)
  }

  def getDeleteRecords(rowTmp:List[org.apache.spark.sql.Row],inputMap:collection.mutable.Map[String,String])=rowTmp.filter(x => jsonStrToMap(x.getAs[String](inputMap("actualMsgColumn")))("CRUDType").asInstanceOf[String]=="Delete" )
  def replicateGrandChild(record:org.apache.spark.sql.Row,inputMap:collection.mutable.Map[String,String])=org.apache.spark.sql.Row(record.schema.map(_.name).foldRight(Seq.empty[Any])((currVar,seqVar) => inputMap("typeFilterColumn")==currVar match {case true =>seqVar:+ inputMap("semIdExamIdExamTypeRefValue") case false =>seqVar:+ record.getAs[String](currVar) } ) :_*)
  def replicateChild(record:org.apache.spark.sql.Row,inputMap:collection.mutable.Map[String,String])=org.apache.spark.sql.Row(record.schema.map(_.name).foldRight(Seq.empty[Any])((currVar,seqVar) => inputMap("typeFilterColumn")==currVar match {case true =>seqVar:+ inputMap("semIdExamIdSubCodeRefValue") case false =>seqVar:+ record.getAs[String](currVar) } ) :_*)

        /*
          assessment is pnt // soft deletes postgres table // scd2 deletes in delta
          childExamAndSub child  // scd2 deletes in delta
          childExamAndType is grand child // scd2 deletes in delta
        */

  // one exam and semID combo can be deleted and inserted only once. // maximum deletes/insert that can happen is 2
  def checkParentRecordRule(key:(String,String),latestRow:org.apache.spark.sql.Row,inputMap:collection.mutable.Map[String,String])= postgresConnection.connection.prepareStatement(s"select count(*) as dataCount from ${inputMap ("postgresTableName")} where sem_id='${key._1}' and exam_id ='${key._2}' and is_valid=true").executeQuery match {
     case value =>
    value.next
    value.getInt("dataCount") match {
        case value if value ==1 =>
          println(s"data true count 1")
          true
        case value if value ==0 =>
          println(s"data true count 0")
          val rsTemp=postgresConnection.connection.prepareStatement(s"select count(*) as dataCount from ${inputMap ("postgresTableName")} where sem_id='${key._1}' and exam_id ='${key._2}' and is_valid=false").executeQuery
          rsTemp.next
          rsTemp.getInt("dataCount") match {
            case value if List(0,1).contains(value) =>
              println(s"data false count 0,1 ${value}")
              true
            case value =>
              println(s"data false count ${value}")
              false
          }
      }
  }

  def softDeleteParentRecord(key:(String,String),inputMap:collection.mutable.Map[String,String])=postgresConnection.connection.prepareStatement(s"update ${inputMap("postgresTableName")} set is_valid=false where sem_id='${key._1}' and exam_id='${key._2}' and is_valid=true").executeUpdate



   def getRow(tmp:Array[Any])=new GenericRowWithSchema(tmp, wrapperSchema)

   def getArrayRow(record:rowStore)=Array(record.messageType,record.actualMessage,record.receivingTimeStamp).map(_.asInstanceOf[Any])

   def getRowWrapper(record:rowStore)=getRow(getArrayRow(record))

  def processIncomingRecords(key:(String,String),incomingRowList: List[org.apache.spark.sql.Row],dataMap:collection.mutable.Map[String,List[org.apache.spark.sql.Row]]=collection.mutable.Map[String,List[org.apache.spark.sql.Row]](),inputMap:collection.mutable.Map[String,String]) ={

    var releasedRows=Seq.empty[org.apache.spark.sql.Row]
   /* val parentRows = incomingRowList.filter(_.getAs[String](inputMap("typeFilterColumn")) == inputMap("assessmentYearRefValue")) ++ (dataMap.get("parent") match {case Some(x) => x case None=> Seq.empty[org.apache.spark.sql.Row]})
    val childExamAndSub = incomingRowList.filter(_.getAs[String](inputMap("typeFilterColumn")) == inputMap("semIdExamIdSubCodeRefValue")) ++ (dataMap.get("child") match {case Some(x) =>  x case None=> Seq.empty[org.apache.spark.sql.Row]})
    val grandChildExamAndType = incomingRowList.filter(_.getAs[String](inputMap("typeFilterColumn")) == inputMap("semIdExamIdExamTypeRefValue")) ++ (dataMap.get("childParent") match {case Some(x) =>  x case None=> Seq.empty[org.apache.spark.sql.Row]})
*/
   val parentRows = (incomingRowList.filter(_.getAs[String](inputMap("typeFilterColumn")) == inputMap("assessmentYearRefValue")) ++ (dataMap.get("parent") match {case Some(x) => x case None=> Seq.empty[org.apache.spark.sql.Row]})).map(x => {println(s"parent records ${x}");x})
    val childExamAndSub = (incomingRowList.filter(_.getAs[String](inputMap("typeFilterColumn")) == inputMap("semIdExamIdSubCodeRefValue")) ++ (dataMap.get("child") match {case Some(x) =>  x case None=> Seq.empty[org.apache.spark.sql.Row]})).map(x => {println(s"child records ${x}");x})
    val grandChildExamAndType = (incomingRowList.filter(_.getAs[String](inputMap("typeFilterColumn")) == inputMap("semIdExamIdExamTypeRefValue")) ++ (dataMap.get("childParent") match {case Some(x) =>  x case None=> Seq.empty[org.apache.spark.sql.Row]})).map(x => {println(s"grand child records ${x}");x})

    parentRows.size match {
      case 0 =>
        childExamAndSub.size match {
          case value if value >= 1 =>
            val childCRUDDeleteCheck=childExamAndSub.filter(x => jsonStrToMap(x.getAs[String](inputMap("actualMsgColumn")))("CRUDType").toString match {case tmp if tmp =="Insert" || tmp == "Update" => false case "Delete"  =>true })
            // delete grandChild too
            childCRUDDeleteCheck.size >=1 match {
              case true =>
                val childLatestRecord=getLatestDeleteRecord(childExamAndSub,inputMap)
                val grandChildLatestRecord=getLatestDeleteRecord(grandChildExamAndType,inputMap)
                getSemIdAndExamIdFromTable(key._1,key._2,inputMap) match {
                  case true =>
                    releasedRows= releasedRows ++ Seq(
                      childLatestRecord,
                      grandChildLatestRecord
                    )
                  case false =>{println(s"No Release, no parent found in msg and table. No state")}
                }

                dataMap.put("child",List(childLatestRecord))
                dataMap.put("grandChild",List(grandChildLatestRecord))

              case false =>
                val childLatestRecord= getLatestRecord(childExamAndSub)
                val grandChildLatestRecord=getLatestRecordWithDeleteCheck(grandChildExamAndType,inputMap)

                getSemIdAndExamIdFromTable(key._1,key._2,inputMap) match {
                  case true =>
                    releasedRows= releasedRows ++ Seq(
                      childLatestRecord,
                      grandChildLatestRecord
                    )
                  case false =>{println(s"No Release, no parent found in msg and table. No state")}
                }

                dataMap.put("child",List(childLatestRecord))
                dataMap.put("grandChild",List(grandChildLatestRecord))
            }
          case 0 =>

            val latestGrandChildRecord=getLatestRecordWithDeleteCheck(grandChildExamAndType,inputMap)
            getSemIdAndExamIdFromTable(key._1,key._2,inputMap) match {
              case true =>
                releasedRows = releasedRows :+ latestGrandChildRecord
              case false => {println("No parent in table, grand child not released")}
            }
            dataMap.put("grandChild",List(latestGrandChildRecord))
        }
      case _ =>  // parent row incoming
        println(s"inside parent row _")
        getDeleteRecords(parentRows,inputMap).size match {
          case 0 =>
            println(s"No deletes for parent")
            import scala.util.control.Breaks._
            breakable {
              val latestParentRecord = getLatestRecord(parentRows)
              println(s"latest parent record ${latestParentRecord}")
              checkParentRecordRule(key,latestParentRecord,inputMap)  match {
                case false =>
                  println(s"parent record rule false ")
                  break
                case true =>
                  println(s"parent record rule true ")
                  (getDeleteRecords(childExamAndSub,inputMap).size, childExamAndSub.size, getDeleteRecords(grandChildExamAndType,inputMap).size, grandChildExamAndType.size) match {
                    case (0, 0, 0, 0) =>
                      dataMap.put("parent", List(latestParentRecord))
                      releasedRows = releasedRows :+ latestParentRecord
                    // nothing to put
                    /*
                   No child , with delete Grand child , without delete grand child
                   delete child  => grandchild delete auto generated
                   i || u  child => with delete Grand child , without delete grand child
                   */

                    case (0, child, 0, grandChild) if child > 0 && grandChild > 0 => // no deletes on both end
                      // latest child and grandchild and parent
                      val latestChildRecord = getLatestRecord(childExamAndSub)
                      val latestGrandChildRecord = getLatestRecord(grandChildExamAndType)

                      dataMap.put("parent", List(latestParentRecord))
                      dataMap.put("child", List(latestChildRecord))
                      dataMap.put("grandChild", List(latestGrandChildRecord))

                      releasedRows = releasedRows :+ latestChildRecord :+ latestGrandChildRecord :+ latestParentRecord
                    case (0, 0, 0, grandChild) if grandChild > 0 =>

                      // latest parent and grandchild , no child

                      val latestGrandChildRecord = getLatestRecord(grandChildExamAndType)

                      dataMap.put("parent", List(latestParentRecord))
                      dataMap.put("grandChild", List(latestGrandChildRecord))

                      releasedRows = releasedRows :+ latestGrandChildRecord :+ latestParentRecord

                    case (0, child, 0, 0) if child > 0 =>
                      //latest parent,child and no grandchild
                      val latestChildRecord = getLatestDeleteRecord(childExamAndSub,inputMap)

                      dataMap.put("parent", List(latestParentRecord))
                      dataMap.put("child", List(latestChildRecord))

                      releasedRows = releasedRows :+ latestParentRecord :+ latestChildRecord

                    case (0, 0, deleteGrandChild, _) if deleteGrandChild > 0 =>
                      //latest parent, no child and delete grandchild
                      val latestGrandChildRecord = getLatestDeleteRecord(grandChildExamAndType,inputMap)

                      dataMap.put("parent", List(latestParentRecord))
                      dataMap.put("grandChild", List(latestGrandChildRecord))

                      releasedRows = releasedRows :+ latestParentRecord :+ latestGrandChildRecord

                    case (deleteChild, _, _, _) if deleteChild > 0 =>
                      //latest parent, delete child and no grandchild // generate grandchild
                      val latestChildRecord = getLatestDeleteRecord(childExamAndSub,inputMap)
                      val latestGrandChildRecord = replicateGrandChild(latestChildRecord,inputMap)

                      dataMap.put("parent", List(latestParentRecord))
                      dataMap.put("child", List(latestChildRecord))
                      dataMap.put("grandChild", List(latestGrandChildRecord))

                      releasedRows = releasedRows :+ latestParentRecord :+ latestGrandChildRecord :+ latestChildRecord
                  }
              }
            }
          case _ =>

            val parentDeleteRecord=getLatestDeleteRecord(parentRows,inputMap)
            dataMap.put("parent",List(parentDeleteRecord))
            softDeleteParentRecord(key,inputMap)

            (childExamAndSub.size,grandChildExamAndType.size) match {
              case (0,0)=>
                val latestGrandChild= replicateGrandChild(parentDeleteRecord,inputMap)
                val latestChild=replicateChild(parentDeleteRecord,inputMap)
                dataMap.put("grandChild",List(latestGrandChild))
                dataMap.put("child",List(latestChild))

                releasedRows = releasedRows :+ parentDeleteRecord :+  latestGrandChild :+ latestChild

              case (0,_)=>

                val latestGrandChild=  getLatestDeleteRecord(grandChildExamAndType,inputMap)
                val latestChild=replicateChild(parentDeleteRecord,inputMap)

                dataMap.put("child",List(latestChild))
                dataMap.put("grandChild",List(latestGrandChild))

                releasedRows = releasedRows :+ parentDeleteRecord :+  latestGrandChild :+ latestChild

              case (_,0)=>

                val latestGrandChild= replicateGrandChild(parentDeleteRecord,inputMap)
                val latestChild = getLatestDeleteRecord(grandChildExamAndType,inputMap)

                dataMap.put("grandChild",List(latestGrandChild))
                dataMap.put("child",List(latestChild))

                releasedRows = releasedRows :+ parentDeleteRecord :+  latestGrandChild :+ latestChild

              case (_,_)=>

                val latestChild = getLatestDeleteRecord(grandChildExamAndType,inputMap)
                val latestGrandChild=  getLatestDeleteRecord(grandChildExamAndType,inputMap)

                dataMap.put("grandChild",List(getLatestDeleteRecord(grandChildExamAndType,inputMap)))
                dataMap.put("child",List(getLatestDeleteRecord(childExamAndSub,inputMap)))

                releasedRows = releasedRows :+ parentDeleteRecord :+  latestGrandChild :+ latestChild

            }
        }
    }

    (releasedRows,dataMap)

  }

  def getRowStore(record:org.apache.spark.sql.Row,inputMap:collection.mutable.Map[String,String])=rowStore(record.getAs[String](inputMap("typeFilterColumn")),record.getAs[String](inputMap("actualMsgColumn")),record.getAs[String]("receivingTimeStamp"))

   def stateFunction(key: (String,String), incomingRowList: List[org.apache.spark.sql.Row],
                    groupState: org.apache.spark.sql.streaming.GroupState[stateStore]
                     ,inputMap:collection.mutable.Map[String,String])
   = groupState.getOption match {
      case Some(state) =>
        println(s"inside Some (State)")
        val (releaseRecords,dataMap)=processIncomingRecords(key,incomingRowList,state.dataMap.map(x => (x._1,x._2.map(getRowWrapper))),inputMap)
        groupState.update(stateStore(dataMap.map(x => (x._1,x._2.map(x => getRowStore(x,inputMap))))))
        releaseRecords
      case None =>
        println(s"inside None")
        val (releaseRecords,dataMap)=processIncomingRecords(key=key,incomingRowList=incomingRowList,inputMap=inputMap)
        groupState.update(stateStore(dataMap.map(x => (x._1,x._2.map(x => getRowStore(x,inputMap))))))
        releaseRecords
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

  def main(args:Array[String]):Unit = {
    val spark = org.apache.spark.sql.SparkSession.builder.getOrCreate
    spark.sparkContext.setLogLevel("ERROR")
    val inputMap=collection.mutable.Map[String,String]()


    for(arg <- args)
    inputMap.put(arg.split("=",2)(0),arg.split("=",2)(1))

    inputMap.foreach(println)

    postgresConnection=connectionHolder(constructConnectionConfig("postgres",inputMap))

    println(s"postgresConnection ${postgresConnection.connection}")

    val readStreamDF=spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers",inputMap("bootstrapServer"))
    .option("startingOffsets",inputMap("startingOffsets"))
    .option("subscribe",inputMap("topic")).load.select(
    org.apache.spark.sql.functions.from_json(org.apache.spark.sql.functions.col("value")
      .cast(org.apache.spark.sql.types.StringType),getSchema(inputMap("outerSchema"))).as("dataExtracted"))
    .select(org.apache.spark.sql.functions.col("dataExtracted.*"))
    .groupByKey(getKey(_,inputMap))(Encoders.product[(String,String)])
   .flatMapGroupsWithState(new flatMapGroupFunction(inputMap),org.apache.spark.sql.streaming.OutputMode.Update,
     Encoders.product[stateStore],org.apache.spark.sql.catalyst.encoders.RowEncoder(wrapperSchema)
    ,GroupStateTimeout.NoTimeout)


    /*.mapGroupsWithState(GroupStateTimeout.NoTimeout)( (key,rowList,state) =>
    stateFunction(key,rowList.toList,state)
  )(org.apache.spark.sql.catalyst.encoders.RowEncoder( new org.apache.spark.sql.types.StructType(
     Array( org.apache.spark.sql.types.StructField("data",wrapperSchema,true ))))*/
           /*
spark-submit --class org.controller.markCalculation.dynamicSchemaSCD2 --packages org.postgresql:postgresql:42.3.5,org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0,io.delta:delta-core_2.12:0.8.0,com.fasterxml.jackson.module:jackson-module-scala_2.12:2.10.0,com.fasterxml.jackson.core:jackson-databind:2.10.0 --num-executors 2 --executor-memory 1g --executor-cores 2 --driver-memory 1g --conf spark.sql.streaming.checkpointLocation=hdfs://localhost:8020/user/raptor/streaming/checkpointLocation/ --driver-cores 2 /home/raptor/IdeaProjects/SparkLearning/build/libs/SparkLearning-1.0-SNAPSHOT.jar typeFilterColumn=messageType assessmentYearRefValue=assessmentInfo semIdExamIdSubCodeRefValue=examAndSubInfo semIdExamIdExamTypeRefValue=examTyeInfo outerSchema=messageType:string~actualMessage:string~receivingTimeStamp:string actualMsgColumn=actualMessage postgresUser=postgres postgresPassword=IAMTHEemperor postgresUrl=jdbc:postgresql://localhost:5432/temp_db postgresDriver=org.postgresql.Driver bootstrapServer=localhost:8081,localhost:8082,localhost:8083 startingOffsets=latest topic=tmpTopic postgresTableName=temp_schema.sem_id_and_exam_id
          actualMsgColumn
           assessmentYearSchema
          outerSchema=messageType:string~actualMessage:string~receivingTimeStamp:string



{"messageType":"assessmentInfo","actualMessage":"{\"assessmentYear\":\"2021-2022\",\"examId\":\"e001\",\"semId\":\"s001\",\"CRUDType\":\"Insert\"}","receivingTimeStamp":"2020-09-09 11:33:44.333"}
{"messageType":"assessmentInfo","actualMessage":"{\"assessmentYear\":\"2021-2022\",\"examId\":\"e001\",\"semId\":\"s001\",\"CRUDType\":\"Delete\"}","receivingTimeStamp":"2020-09-10 11:33:44.333"}
{"messageType":"assessmentInfo","actualMessage":"{\"assessmentYear\":\"2021-2022\",\"examId\":\"e001\",\"semId\":\"s001\",\"CRUDType\":\"Insert\"}","receivingTimeStamp":"2020-09-11 11:33:44.333"}
{"messageType":"assessmentInfo","actualMessage":"{\"assessmentYear\":\"2021-2022\",\"examId\":\"e001\",\"semId\":\"s001\",\"CRUDType\":\"Delete\"}","receivingTimeStamp":"2020-09-12 11:33:44.333"}
{"messageType":"assessmentInfo","actualMessage":"{\"assessmentYear\":\"2021-2022\",\"examId\":\"e001\",\"semId\":\"s001\",\"CRUDType\":\"Insert\"}","receivingTimeStamp":"2020-09-13 11:33:44.333"}
          */

    readStreamDF.writeStream.format("console").outputMode("update")
      .option("truncate","false").start

  /*
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


/*def processIncomingRecords(key:(String,String),incomingRowList: List[org.apache.spark.sql.Row],dataMap:collection.mutable.Map[String,List[org.apache.spark.sql.Row]]=collection.mutable.Map[String,List[org.apache.spark.sql.Row]](),inputMap:collection.mutable.Map[String,String]) ={

    var releasedRows=Seq.empty[org.apache.spark.sql.Row]
    val parentRows = incomingRowList.filter(_.getAs[String](inputMap("typeFilterColumn")) == inputMap("assessmentYearRefValue")) ++ (dataMap.get("parent") match {case Some(x) => x case None=> Seq.empty[org.apache.spark.sql.Row]})
    val childExamAndSub = incomingRowList.filter(_.getAs[String](inputMap("typeFilterColumn")) == inputMap("semIdExamIdSubCodeRefValue")) ++ (dataMap.get("child") match {case Some(x) => x case None=> Seq.empty[org.apache.spark.sql.Row]})
    val grandChildExamAndType = incomingRowList.filter(_.getAs[String](inputMap("typeFilterColumn")) == inputMap("semIdExamIdExamTypeRefValue")) ++ (dataMap.get("childParent") match {case Some(x) => x case None=> Seq.empty[org.apache.spark.sql.Row]})

    parentRows.size match {
      case 0 =>
        childExamAndSub.size match {
          case value if value >= 1 =>
            val childCRUDDeleteCheck=childExamAndSub.filter(x => jsonStrToMap(x.getAs[String](inputMap("actualMsgColumn")))("CRUDType").toString match {case tmp if tmp =="Insert" || tmp == "Update" => false case "Delete"  =>true })
            // delete grandChild too
            childCRUDDeleteCheck.size >=1 match {
              case true =>
                val childLatestRecord=getLatestDeleteRecord(childExamAndSub,inputMap)
                val grandChildLatestRecord=getLatestDeleteRecord(grandChildExamAndType,inputMap)
                getSemIdAndExamIdFromTable(key._1,key._2,inputMap) match {
                  case true =>
                    releasedRows= releasedRows ++ Seq(
                      childLatestRecord,
                      grandChildLatestRecord
                    )
                  case false =>{println(s"No Release, no parent found in msg and table. No state")}
                }

                dataMap.put("child",List(childLatestRecord))
                dataMap.put("grandChild",List(grandChildLatestRecord))

              case false =>
                val childLatestRecord= getLatestRecord(childExamAndSub)
                val grandChildLatestRecord=getLatestRecordWithDeleteCheck(grandChildExamAndType,inputMap)

                getSemIdAndExamIdFromTable(key._1,key._2,inputMap) match {
                  case true =>
                    releasedRows= releasedRows ++ Seq(
                      childLatestRecord,
                      grandChildLatestRecord
                    )
                  case false =>{println(s"No Release, no parent found in msg and table. No state")}
                }

                dataMap.put("child",List(childLatestRecord))
                dataMap.put("grandChild",List(grandChildLatestRecord))
            }
          case 0 =>

            val latestGrandChildRecord=getLatestRecordWithDeleteCheck(grandChildExamAndType,inputMap)
            getSemIdAndExamIdFromTable(key._1,key._2,inputMap) match {
              case true =>
                releasedRows = releasedRows :+ latestGrandChildRecord
              case false => {println("No parent in table, grand child not released")}
            }
            dataMap.put("grandChild",List(latestGrandChildRecord))
        }
      case _ =>  // parent row incoming
        getDeleteRecords(parentRows,inputMap).size match {
          case 0 =>
            import scala.util.control.Breaks._
            breakable {
              val latestParentRecord = getLatestRecord(parentRows)
              checkParentRecordRule(key,latestParentRecord,inputMap)  match {
                case false => break
                case true =>
                  (getDeleteRecords(childExamAndSub,inputMap).size, childExamAndSub.size, getDeleteRecords(grandChildExamAndType,inputMap).size, grandChildExamAndType.size) match {
                    case (0, 0, 0, 0) => {}
                    // nothing to put
                    /*
                   No child , with delete Grand child , without delete grand child
                   delete child  => grandchild delete auto generated
                   i || u  child => with delete Grand child , without delete grand child
                   */

                    case (0, child, 0, grandChild) if child > 0 && grandChild > 0 => // no deletes on both end
                      // latest child and grandchild and parent
                      val latestChildRecord = getLatestRecord(childExamAndSub)
                      val latestGrandChildRecord = getLatestRecord(grandChildExamAndType)

                      dataMap.put("parent", List(latestParentRecord))
                      dataMap.put("child", List(latestChildRecord))
                      dataMap.put("grandChild", List(latestGrandChildRecord))

                      releasedRows = releasedRows :+ latestChildRecord :+ latestGrandChildRecord :+ latestParentRecord
                    case (0, 0, 0, grandChild) if grandChild > 0 =>

                      // latest parent and grandchild , no child

                      val latestGrandChildRecord = getLatestRecord(grandChildExamAndType)

                      dataMap.put("parent", List(latestParentRecord))
                      dataMap.put("grandChild", List(latestGrandChildRecord))

                      releasedRows = releasedRows :+ latestGrandChildRecord :+ latestParentRecord

                    case (0, child, 0, 0) if child > 0 =>
                      //latest parent,child and no grandchild
                      val latestChildRecord = getLatestDeleteRecord(childExamAndSub,inputMap)

                      dataMap.put("parent", List(latestParentRecord))
                      dataMap.put("child", List(latestChildRecord))

                      releasedRows = releasedRows :+ latestParentRecord :+ latestChildRecord

                    case (0, 0, deleteGrandChild, _) if deleteGrandChild > 0 =>
                      //latest parent, no child and delete grandchild
                      val latestGrandChildRecord = getLatestDeleteRecord(grandChildExamAndType,inputMap)

                      dataMap.put("parent", List(latestParentRecord))
                      dataMap.put("grandChild", List(latestGrandChildRecord))

                      releasedRows = releasedRows :+ latestParentRecord :+ latestGrandChildRecord

                    case (deleteChild, _, _, _) if deleteChild > 0 =>
                      //latest parent, delete child and no grandchild // generate grandchild
                      val latestChildRecord = getLatestDeleteRecord(childExamAndSub,inputMap)
                      val latestGrandChildRecord = replicateGrandChild(latestChildRecord,inputMap)

                      dataMap.put("parent", List(latestParentRecord))
                      dataMap.put("child", List(latestChildRecord))
                      dataMap.put("grandChild", List(latestGrandChildRecord))

                      releasedRows = releasedRows :+ latestParentRecord :+ latestGrandChildRecord :+ latestChildRecord
                  }
              }
            }
          case _ =>

            val parentDeleteRecord=getLatestDeleteRecord(parentRows,inputMap)
            dataMap.put("parent",List(parentDeleteRecord))
            softDeleteParentRecord(key,inputMap)

            (childExamAndSub.size,grandChildExamAndType.size) match {
              case (0,0)=>
                val latestGrandChild= replicateGrandChild(parentDeleteRecord,inputMap)
                val latestChild=replicateChild(parentDeleteRecord,inputMap)
                dataMap.put("grandChild",List(latestGrandChild))
                dataMap.put("child",List(latestChild))

                releasedRows = releasedRows :+ parentDeleteRecord :+  latestGrandChild :+ latestChild

              case (0,_)=>

                val latestGrandChild=  getLatestDeleteRecord(grandChildExamAndType,inputMap)
                val latestChild=replicateChild(parentDeleteRecord,inputMap)

                dataMap.put("child",List(latestChild))
                dataMap.put("grandChild",List(latestGrandChild))

                releasedRows = releasedRows :+ parentDeleteRecord :+  latestGrandChild :+ latestChild

              case (_,0)=>

                val latestGrandChild= replicateGrandChild(parentDeleteRecord,inputMap)
                val latestChild = getLatestDeleteRecord(grandChildExamAndType,inputMap)

                dataMap.put("grandChild",List(latestGrandChild))
                dataMap.put("child",List(latestChild))

                releasedRows = releasedRows :+ parentDeleteRecord :+  latestGrandChild :+ latestChild

              case (_,_)=>

                val latestChild = getLatestDeleteRecord(grandChildExamAndType,inputMap)
                val latestGrandChild=  getLatestDeleteRecord(grandChildExamAndType,inputMap)

                dataMap.put("grandChild",List(getLatestDeleteRecord(grandChildExamAndType,inputMap)))
                dataMap.put("child",List(getLatestDeleteRecord(childExamAndSub,inputMap)))

                releasedRows = releasedRows :+ parentDeleteRecord :+  latestGrandChild :+ latestChild

            }
        }
    }

    (releasedRows,dataMap)

  }*/
