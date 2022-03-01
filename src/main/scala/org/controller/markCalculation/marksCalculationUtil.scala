package org.controller.markCalculation

import org.apache.spark.sql.streaming.DataStreamWriter
import org.controller.markCalculation.marksCalculationConstant._

import scala.util.{Failure, Success, Try}
import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import org.apache.spark.TaskContext
import org.apache.spark.sql.functions.{col, from_json, udf}


object marksCalculationUtil extends Serializable{

  def inputArrayToMap(args:Array[String])={
    val inputMap=collection.mutable.Map[String,String]()
    for(arg <- args)
      addElementToMap(arg,inputMap)
    inputMap
  }

  def getSparkSession(conf:org.apache.spark.SparkConf=null)= conf match {
    case null => org.apache.spark.sql.SparkSession.builder.enableHiveSupport.getOrCreate
    case value => org.apache.spark.sql.SparkSession.builder.enableHiveSupport.config(value).getOrCreate
  }


  def addElementToMap(arg:String,tmpMap:collection.mutable.Map[String,String])= splitArg(arg) match {
   case value if value.size ==2 =>
     tmpMap.put(keyGetter(value) ,valGetter(value))
     tmpMap
   case _ =>tmpMap
 }

  def getReadStreamDFFun (spark:org.apache.spark.sql.SparkSession,inputMap:collection.mutable.Map[String,String]) =
 Try{inputMap(readStreamFormat)} match {
   case Success(s)=>
     println(s"getReadStreamDFFun :: Success")
     s match {
     case value if value == deltaStreamFormat =>
       println(s"getReadStreamDFFun :: Success delta")
       spark.readStream.format("delta").load(inputMap(pathArg))
     case value if value == kafkaStreamFormat =>
       println(s"getReadStreamDFFun :: Success kafka")
      // println(s"getReadStreamDF :: Success kafka :: kafkaStreamFormat ${getSubscribeAssignValue(inputMap)}")
      // println(s"getReadStreamDF :: Success kafka :: kafkaSubscribeAssignDecider ${subscribeAssignDecider(inputMap(kafkaSubscribeAssignDecider))}")
       spark.readStream.format("kafka")
         .option("kafka.bootstrap.servers",inputMap(kafkaBootstrapServerArg))
         .option(subscribeAssignDecider(inputMap(kafkaSubscribeAssignDecider)),getSubscribeAssignValue(inputMap))
         .option("startingOffsets",inputMap(kafkaStartingOffsetsArg))
         .load
   }
   case Failure(f)=> // reads parquet
     println(s"getReadStreamDFFun :: Failure")
     spark.readStream.load(inputMap(pathArg))
 }

  def getWhereCondition(valueArray:Array[String])=
    /*    var resultStr=""
    valueArray.size match {
        case 0 => resultStr="''"
        case value if value >0 =>
          for (valueInArray <- valueArray)
          resultStr.trim.size match {
            case value if value ==0 => resultStr+=s"'${valueInArray}'"
            case value if value > 0 => resultStr+=s",'${valueInArray}'"
          }
      }*/
    (valueArray.size match {
      case 0 => Array("''")
      case value if value >0 =>
        for(value <- valueArray)
          yield {s"'${value}'"}
    } ).mkString("(",",",")")



  val getDeltaTable:(org.apache.spark.sql.SparkSession,String)=>io.delta.tables.DeltaTable=
    (spark:org.apache.spark.sql.SparkSession,deltaTablePath:String) =>
  io.delta.tables.DeltaTable.forPath(spark,deltaTablePath)


  def getBigDecimalFromRow(row:org.apache.spark.sql.Row,fieldName:String)=convertToScalaBigDecimal(row.getAs[java.math.BigDecimal](fieldName))
  val convertToScalaBigDecimal:(java.math.BigDecimal)=> scala.math.BigDecimal= (javaBigD:java.math.BigDecimal) => scala.math.BigDecimal(javaBigD)

//  val getTotalGrade(totalMarks:)

def getGrade(maxMarks:scala.math.BigDecimal,marksObtained:scala.math.BigDecimal,examType:String="CA")= (marksObtained * (100/ maxMarks)) match {
  case value => examType match {
    case assessmentType if assessmentType == cumulativeAssessment =>
      value match {
        case value if value > scala.math.BigDecimal(95.0) => "A+"
        case value if value > scala.math.BigDecimal(90.0) => "A"
        case value if value > scala.math.BigDecimal(85.0) => "B"
        case value if value > scala.math.BigDecimal(80.0) => "B+"
        case value if value > scala.math.BigDecimal(75.0) => "C"
        case value if value > scala.math.BigDecimal(70.0) => "C+"
        case value if value > scala.math.BigDecimal(65.0) => "D"
        case value if value > scala.math.BigDecimal(60.0) => "D+"
        case value if value > scala.math.BigDecimal(50.0) => "E"
        case value if value <= scala.math.BigDecimal(50.0) => "E+"
        case value if value < scala.math.BigDecimal(45.0) => "F"
      }
    case assessmentType if assessmentType == summativeAssessment =>
      value match {
        case value if value > scala.math.BigDecimal(95.0) => "A+"
        case value if value > scala.math.BigDecimal(90.0) => "A"
        case value if value > scala.math.BigDecimal(85.0) => "B"
        case value if value > scala.math.BigDecimal(80.0) => "B+"
        case value if value > scala.math.BigDecimal(75.0) => "C"
        case value if value > scala.math.BigDecimal(70.0) => "C+"
        case value if value > scala.math.BigDecimal(65.0) => "D"
        case value if value > scala.math.BigDecimal(60.0) => "E"
        case value if value <= scala.math.BigDecimal(60.0) => "E+"
        case value if value < scala.math.BigDecimal(50.0) => "F"
      }
  }
}

  def getGradeJava(maxMarks:scala.math.BigDecimal,marksObtained:java.math.BigDecimal,examType:String="CA")=
    marksObtained.multiply(new java.math.BigDecimal((100/ maxMarks).toInt)) match {
 //  scala.math.BigDecimal(marksObtained.toString.toDouble) / (100/ maxMarks) match {
    case value => examType match {
      case assessmentType if assessmentType == cumulativeAssessment =>
        scala.math.BigDecimal(value) match {
          case value if value > scala.math.BigDecimal(95.0) => "A+"
          case value if value > scala.math.BigDecimal(90.0) => "A"
          case value if value > scala.math.BigDecimal(85.0) => "B"
          case value if value > scala.math.BigDecimal(80.0) => "B+"
          case value if value > scala.math.BigDecimal(75.0) => "C"
          case value if value > scala.math.BigDecimal(70.0) => "C+"
          case value if value > scala.math.BigDecimal(65.0) => "D"
          case value if value > scala.math.BigDecimal(60.0) => "D+"
          case value if value > scala.math.BigDecimal(50.0) => "E"
          case value if value < scala.math.BigDecimal(45.0) => "F"
        }
      case assessmentType if assessmentType == summativeAssessment =>
        value match {
          case value if value.compareTo(new java.math.BigDecimal(95.0)) == 1 => "A+"
          case value if value.compareTo(new java.math.BigDecimal(90.0)) == 1 => "A"
          case value if value.compareTo(new java.math.BigDecimal(85.0)) == 1 => "B"
          case value if value.compareTo(new java.math.BigDecimal(80.0)) == 1 => "B+"
          case value if value.compareTo(new java.math.BigDecimal(75.0)) == 1 => "C"
          case value if value.compareTo(new java.math.BigDecimal(70.0)) == 1 => "C+"
          case value if value.compareTo(new java.math.BigDecimal(65.0)) == 1 => "D"
          case value if value.compareTo(new java.math.BigDecimal(60.0)) == 1 => "E"
          case value if value.compareTo(new java.math.BigDecimal(50.0)) == -1 => "F"
        /*  case value if value > scala.math.BigDecimal(95.0)=> "A+"
          case value if value > scala.math.BigDecimal(90.0)=> "A"
          case value if value > scala.math.BigDecimal(85.0)=> "B"
          case value if value > scala.math.BigDecimal(80.0)=> "B+"
          case value if value > scala.math.BigDecimal(75.0)=> "C"
          case value if value > scala.math.BigDecimal(70.0)=> "C+"
          case value if value > scala.math.BigDecimal(65.0)=> "D"
          case value if value > scala.math.BigDecimal(60.0)=> "E"
          case value if value < scala.math.BigDecimal(50.0) => "F" */
        }
    }
  }



  def getGradeJavaUpdated(maxMarks:java.math.BigDecimal,marksObtained:java.math.BigDecimal,examType:String="CA")=
    marksObtained.multiply(new java.math.BigDecimal(100).divide(maxMarks)) match {
      case value => examType match {
        case assessmentType if assessmentType == cumulativeAssessment =>
          value match {
            case value if value.compareTo(new java.math.BigDecimal(95.0)) == 1 => "A+"
            case value if value.compareTo(new java.math.BigDecimal(90.0)) == 1 => "A"
            case value if value.compareTo(new java.math.BigDecimal(85.0)) == 1 => "B"
            case value if value.compareTo(new java.math.BigDecimal(80.0)) == 1 => "B+"
            case value if value.compareTo(new java.math.BigDecimal(75.0)) == 1 => "C"
            case value if value.compareTo(new java.math.BigDecimal(70.0)) == 1 => "C+"
            case value if value.compareTo(new java.math.BigDecimal(65.0)) == 1 => "D"
            case value if value.compareTo(new java.math.BigDecimal(60.0)) == 1 => "D+"
            case value if value.compareTo(new java.math.BigDecimal(50.0)) == 1 => "E"
            case value if List(0,1).contains(value.compareTo(new java.math.BigDecimal(45.0))) => "E+"
            case value if value.compareTo(new java.math.BigDecimal(45.0)) == -1 => "F"
          }
        case assessmentType if assessmentType == summativeAssessment =>
          value match {
            case value if value.compareTo(new java.math.BigDecimal(95.0)) == 1 => "A+"
            case value if value.compareTo(new java.math.BigDecimal(90.0)) == 1 => "A"
            case value if value.compareTo(new java.math.BigDecimal(85.0)) == 1 => "B"
            case value if value.compareTo(new java.math.BigDecimal(80.0)) == 1 => "B+"
            case value if value.compareTo(new java.math.BigDecimal(75.0)) == 1 => "C"
            case value if value.compareTo(new java.math.BigDecimal(70.0)) == 1 => "C+"
            case value if value.compareTo(new java.math.BigDecimal(65.0)) == 1 => "D"
            case value if value.compareTo(new java.math.BigDecimal(60.0)) == 1 => "E"
            case value if Array(0,1).contains(value.compareTo(new java.math.BigDecimal(50.0))) => "E+"
            case value if value.compareTo(new java.math.BigDecimal(50.0)) == -1 => "F"

          }
      }
    }

  def getGradeJavaNew(maxMarks:String,marksObtained:Float,examType:String)= marksObtained / (100/ maxMarks.toFloat) match {
    case value => examType match {
      case assessmentType if assessmentType == cumulativeAssessment =>
        value match {
          case value if value > 95.0F => "A+"
          case value if value > 90.0F => "A"
          case value if value > 85.0F => "B"
          case value if value > 80.0F => "B+"
          case value if value > 75.0F => "C"
          case value if value > 70.0F => "C+"
          case value if value > 65.0F => "D"
          case value if value > 60.0F => "D+"
          case value if value > 50.0F => "E"
          case value if value >= 45.0F => "E+"
          case value if value < 45.0F => "F"
        }
      case assessmentType if assessmentType == summativeAssessment =>
        value match {
          case value if value > 95.0 => "A+"
          case value if value > 90.0 => "A"
          case value if value > 85.0 => "B"
          case value if value > 80.0 => "B+"
          case value if value > 75.0 => "C"
          case value if value > 70.0 => "C+"
          case value if value > 65.0 => "D"
          case value if value > 60.0 => "E"
          case value if value > 50.0F => "E+"
          case value if value <= 50.0=> "F"
        }
    }
  }

  def getPassMarkPercentage(inputMap:collection.mutable.Map[String,String])= inputMap("examType") match {
    case value if value == summativeAssessment => 50
    case value if value == cumulativeAssessment => 45
  }

  def getPassMarkPercentage(examType:String)= examType match {
    case value if value == summativeAssessment => 50
    case value if value == cumulativeAssessment => 45
  }


  val getReadStreamDF:(org.apache.spark.sql.SparkSession,collection.mutable.Map[String,String])=> org.apache.spark.sql.DataFrame = (spark:org.apache.spark.sql.SparkSession,inputMap:collection.mutable.Map[String,String]) =>
    Try{inputMap(readStreamFormat)} match {
      case Success(s)=>
        println(s"getReadStreamDF :: Success")
        s match {
          case value if value == deltaStreamFormat =>
            println(s"getReadStreamDF :: Success delta")
            Try{inputMap(deltaIgnoreChanges)} match {
              case Success(s) =>
                println(s"getReadStreamDF :: Success delta ignore changes")
                spark.readStream.format("delta").option("ignoreChanges","true").load(inputMap(pathArg))
              case Failure(f) =>
                println(s"getReadStreamDF :: Failure delta ignore changes")
                spark.readStream.format("delta").load(inputMap(pathArg))
            }
          case value if value == kafkaStreamFormat =>
            println(s"getReadStreamDF :: Success kafka")
            // println(s"getReadStreamDF :: Success kafka :: kafkaStreamFormat ${getSubscribeAssignValue(inputMap)}")
            // println(s"getReadStreamDF :: Success kafka :: kafkaSubscribeAssignDecider ${subscribeAssignDecider(inputMap(kafkaSubscribeAssignDecider))}")
            spark.readStream.format("kafka")
              .option("kafka.bootstrap.servers",inputMap(kafkaBootstrapServerArg))
              .option(subscribeAssignDecider(inputMap(kafkaSubscribeAssignDecider)),getSubscribeAssignValue(inputMap))
              .option("startingOffsets",inputMap(kafkaStartingOffsetsArg))
              .load
        }
      case Failure(f)=> // reads parquet
        println(s"getReadStreamDF :: Failure")
        spark.readStream.load(inputMap(pathArg))
    }
  val getSubscribeAssignValue:(collection.mutable.Map[String,String])=>String=(map:collection.mutable.Map[String,String])=> Try{map(kafkaSubscribeAssignDecider)} match {
    case Success(s) =>
      s match {case value if value ==kafkaSubscribe => map(kafkaSubscribe) case value if value ==kafkaAssign => map(kafkaAssign)}
    case Failure(f) =>""
  }

  def subscribeAssignDecider(deciderString:String)=deciderString match {
    case value if value == kafkaSubscribe => "subscribe"
    case value if value == kafkaAssign => "assign"
  }

  val dfWriterStream:(org.apache.spark.sql.SparkSession,org.apache.spark.sql.DataFrame,collection.mutable.Map[String,String])=>Unit=
    (spark:org.apache.spark.sql.SparkSession,df:org.apache.spark.sql.DataFrame,tmpMap:collection.mutable.Map[String,String]) => Try{tmpMap(writeStreamFormat)} match {
    case Success(s) =>
      println("dfWriterStream Success")
      s match {
      case value if value == deltaStreamFormat =>
        println("dfWriterStream Success delta")
        tmpMap(deltaMergeOverwriteDecider) match {
          case value if value == deltaMerge =>
            println("dfWriterStream Success delta merge")
            df.write.format("delta").mode("append").option("mergeSchema",deltaMergeOverwriteHelper(tmpMap,deltaMerge)).save(tmpMap("path"))
          case value if value == deltaOverwrite =>
            println("dfWriterStream Success delta overwrite")
            df.write.format("delta").mode("append").option("overwriteSchema",deltaMergeOverwriteHelper(tmpMap,deltaOverwrite)).save(tmpMap("path"))
          case value if value == deltaMergeAndOverwrite =>
            println("dfWriterStream Success delta merge and overwrite")
            df.write.format("delta").mode("append").option("mergeSchema",deltaMergeOverwriteHelper(tmpMap,deltaMerge)).option("overwriteSchema",deltaMergeOverwriteHelper(tmpMap,deltaOverwrite)).save(tmpMap("path"))
        }
      case value if value == kafkaStreamFormat =>
        println("dfWriterStream Success kafka")
       import spark.implicits._
     //   df.map(x=> kafkaWrapper(x)).map(toJson).writeStream.format("kafka").option("kafka.bootstrap.servers",tmpMap(kafkaBootstrapServerArg)).option("topic",tmpMap(kafkaTopic)).option("checkpointLocation",tmpMap(checkpointLocation)).start
    /*    import org.apache.spark.sql.catalyst.encoders.RowEncoder
        implicit val encoder = RowEncoder(df.head.schema) */
        df.map(x=> toJsonWrapper(x)).write.mode("append").format("kafka").option("kafka.bootstrap.servers",tmpMap(kafkaBootstrapServerArg)).option("topic",tmpMap(kafkaTopic)).save
      }
    case Failure(f) =>
      println("dfWriterStream Failure")
      df.write.mode("append").save(tmpMap("path"))
    }


  def toJsonWrapper(row:org.apache.spark.sql.Row)={
    val rowSchema=row.schema
    var stringTmp=""
    for (rowInfo<- rowSchema)
        stringTmp= stringTmp.trim.size match {case value if value >0 => stringTmp+s""","${rowInfo.name}":${valueCalculator(rowInfo.dataType) match {case true => s""""${row.getAs[String](rowInfo.name)}"""" case false => s"""${row.getAs[String](rowInfo.name)}""" }}""" case value if value == 0 =>s""""${rowInfo.name}":${valueCalculator(rowInfo.dataType) match {case true => s""""${row.getAs[String](rowInfo.name)}"""" case false => s"""${row.getAs[String](rowInfo.name)}""" }}"""}
    s"{${stringTmp}}"
   }


  def valueCalculator(typeField:org.apache.spark.sql.types.DataType)= typeField match {
    case org.apache.spark.sql.types.StringType => true
    case org.apache.spark.sql.types.IntegerType => false
    case org.apache.spark.sql.types.FloatType => false
    case org.apache.spark.sql.types.DecimalType() => false
    case org.apache.spark.sql.types.TimestampType => true
    case org.apache.spark.sql.types.DateType => true
  }

  def deltaMergeOverwriteHelper(map:collection.mutable.Map[String,String],getStr:String) =  Try{map(getStr)} match { case Success(s) => s case Failure(f) => "false"}

  def keyGetter(argSplit:Array[String])=  Try{argSplit(0)} match {case Success(s) => s case Failure(f) => ""}

  val valGetter:(Array[String])=> String = (argSplit:Array[String]) =>  Try{argSplit(1)} match {case Success(s) => s case Failure(f) => ""}
  val splitArg:(String)=>Array[String] = (arg:String)=> Try{arg.split("=",2)} match {case Success(s) => s case Failure(f) => Array.empty}

  val batchIdUDF = udf { () =>
    TaskContext.get.getLocalProperty("streaming.sql.batchId").toInt
  }
  val persistDF:(org.apache.spark.sql.DataFrame,collection.mutable.Map[String,String])=> Unit = (df:org.apache.spark.sql.DataFrame,mapTmp:collection.mutable.Map[String,String]) => saveDF(df.write.mode(mapTmp("writeMode")).format(mapTmp("writeFormat")),mapTmp("writePath"))

  def saveDF(dfWriter:org.apache.spark.sql.DataFrameWriter[org.apache.spark.sql.Row],path:String)= dfWriter.save(path)

  val mapper=new ObjectMapper() with ScalaObjectMapper
  mapper.registerModule(DefaultScalaModule)
  mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES,false)

  def toJson(value:Any)= mapper.writeValueAsString(value)

  def fromJson[T](json:String)(implicit m:Manifest[T])=mapper.readValue[T](json)


  val simpleDateFormat= new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
  def getTSFromString(tsStr:String)=new java.sql.Timestamp(simpleDateFormat.parse(tsStr).getTime)

  val udfTSFromString= udf(getTSFromString(_:String))
  def innerMsgParser(df:org.apache.spark.sql.DataFrame)=df.select(from_json(col("actualMessage"),innerMarksSchema).as("structExtracted"),udfTSFromString(col("receivingTimeStamp")).as("incomingTS")).select(col("structExtracted.*"),col("incomingTS"))

}

case class kafkaWrapper(value:org.apache.spark.sql.Row)


case class tmpD(cool:String,cool2:String)

object tmpD {
def apply(cool:String,cool2:String):tmpD = tmpD(cool,cool2)
}
