package org.controller.markCalculation

import org.controller.markCalculation.marksCalculationConstant._

import scala.util.{Failure, Success, Try}
import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import org.apache.spark.TaskContext
import org.apache.spark.sql.functions.{col, from_json, udf}

import java.math.MathContext


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

  def getReadStreamDFFun (spark:org.apache.spark.sql.SparkSession,inputMap:collection.mutable.Map[String,String]) = Try{inputMap(readStreamFormat)} match {
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
      }
      */
    (valueArray.size match {
      case 0 => Array("''")
      case value if value >0 =>
        for(value <- valueArray)
          yield {s"'${value}'"}
    } ).mkString("(",",",")")



  val getDeltaTable:(org.apache.spark.sql.SparkSession,String)=>io.delta.tables.DeltaTable=
    (spark:org.apache.spark.sql.SparkSession,deltaTablePath:String) =>
  io.delta.tables.DeltaTable.forPath(spark,deltaTablePath)

   def getBigDecimalFromRow(row:org.apache.spark.sql.Row,columnName:String)= row.getAs[java.math.BigDecimal](columnName)

  def getBigDecimalFromRowAsScalaBD(row:org.apache.spark.sql.Row, fieldName:String)=convertToScalaBigDecimal(row.getAs[java.math.BigDecimal](fieldName))
  val convertToScalaBigDecimal:(java.math.BigDecimal)=> scala.math.BigDecimal= (javaBigD:java.math.BigDecimal) => scala.math.BigDecimal(javaBigD)

//  val getTotalGrade(totalMarks:)

def getGrade(maxMarks:scala.math.BigDecimal,marksObtained:scala.math.BigDecimal,examType:String="CA")= {
  (marksObtained / (maxMarks/100 )) match { // (marksObtained * (100/maxMarks ))
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
        case value if value > scala.math.BigDecimal(55.0) => "E"
        case value if value >= scala.math.BigDecimal(45.0) => "E+"
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
        case value if value >= scala.math.BigDecimal(50.0) => "E+"
        case value if value < scala.math.BigDecimal(50.0) => "F"
      }
    case assessmentType if assessmentType == "finalCalculation" =>
      value match {
        case value if value > scala.math.BigDecimal(95.0) => "A+"
        case value if value > scala.math.BigDecimal(90.0) => "A"
        case value if value > scala.math.BigDecimal(85.0) => "B"
        case value if value > scala.math.BigDecimal(80.0) => "B+"
        case value if value > scala.math.BigDecimal(75.0) => "C"
        case value if value > scala.math.BigDecimal(70.0) => "C+"
        case value if value > scala.math.BigDecimal(65.0) => "D"
        case value if value > scala.math.BigDecimal(60.0) => "E"
        case value if value >= scala.math.BigDecimal(50.0) => "E+"
        case value if value < scala.math.BigDecimal(50.0) => "F"
      }
  }
}
}
  // depreciated

  def getGradeHybrid(maxMarks:scala.math.BigDecimal,marksObtained:java.math.BigDecimal,examType:String="CA")=
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


  def getGradeJava(maxMarks:java.math.BigDecimal,marksObtained:java.math.BigDecimal,examType:String="CA")=
    marksObtained.multiply(new java.math.BigDecimal(100.0).divide(maxMarks,MathContext.DECIMAL128),MathContext.DECIMAL128) match {
      //  scala.math.BigDecimal(marksObtained.toString.toDouble) / (100/ maxMarks) match {
      case value => examType match {
        case assessmentType if assessmentType == cumulativeAssessment =>
          value match {
            case value if value.compareTo(new java.math.BigDecimal(95.0)) == 1 => "A+"
            case value if value.compareTo(new java.math.BigDecimal(90.0)) == 1 => "A"
            case value if value.compareTo(new java.math.BigDecimal(85.0)) == 1 => "B+"
            case value if value.compareTo(new java.math.BigDecimal(80.0)) == 1 => "B"
            case value if value.compareTo(new java.math.BigDecimal(75.0)) == 1 => "C+"
            case value if value.compareTo(new java.math.BigDecimal(70.0)) == 1 => "C"
            case value if value.compareTo(new java.math.BigDecimal(65.0)) == 1 => "D+"
            case value if value.compareTo(new java.math.BigDecimal(60.0)) == 1 => "D"
            case value if value.compareTo(new java.math.BigDecimal(50.0)) == 1 => "E+"
            case value if List(0,1).contains( value.compareTo(new java.math.BigDecimal(45.0))) => "E"
            case value if value.compareTo(new java.math.BigDecimal(45.0)) == -1  => "F"
          }
        case assessmentType if assessmentType == summativeAssessment =>
          value match {
            case value if value.compareTo(new java.math.BigDecimal(95.0)) == 1 => "A+"
            case value if value.compareTo(new java.math.BigDecimal(90.0)) == 1 => "A"
            case value if value.compareTo(new java.math.BigDecimal(85.0)) == 1 => "B+"
            case value if value.compareTo(new java.math.BigDecimal(80.0)) == 1 => "B"
            case value if value.compareTo(new java.math.BigDecimal(75.0)) == 1 => "C+"
            case value if value.compareTo(new java.math.BigDecimal(70.0)) == 1 => "C"
            case value if value.compareTo(new java.math.BigDecimal(65.0)) == 1 => "D+"
            case value if value.compareTo(new java.math.BigDecimal(60.0)) == 1 => "D"
            case value if List(0,1).contains( value.compareTo(new java.math.BigDecimal(50.0))) => "E"
            case value if value.compareTo(new java.math.BigDecimal(50.0)) == -1 => "F"
          }
       case assessmentType if assessmentType == "finalCalculation" =>
          value match {
            case value if value.compareTo(new java.math.BigDecimal(95.0)) == 1 => "A+"
            case value if value.compareTo(new java.math.BigDecimal(90.0)) == 1 => "A"
            case value if value.compareTo(new java.math.BigDecimal(85.0)) == 1 => "B+"
            case value if value.compareTo(new java.math.BigDecimal(80.0)) == 1 => "B"
            case value if value.compareTo(new java.math.BigDecimal(75.0)) == 1 => "C"
            case value if value.compareTo(new java.math.BigDecimal(70.0)) == 1 => "D"
            case value if List(0,1).contains( value.compareTo(new java.math.BigDecimal(60.0))) => "E"
            case value if value.compareTo(new java.math.BigDecimal(0.0)) == 0  => "F-"
            case value if value.compareTo(new java.math.BigDecimal(60.0)) == -1 => "F"
          }
      }
    }

  def array_filter_contains[T](arrayCol:Seq[T],filterObj:T) = arrayCol.filter(_.toString.contains(filterObj.toString))

  def array_filter_equals[T](arrayCol:Seq[T],filterObj:T):Seq[T] = arrayCol.filter(_.equals(filterObj))



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


  def getSACACols(colArray:Array[String]) = for(colTmp<- colArray) yield Seq(col(s"sa.${colTmp}").as(s"sa_${colTmp}"),col(s"ca.${colTmp}").as(s"ca_${colTmp}"))
  val getCASACols : (Array[String]) => Seq[org.apache.spark.sql.Column] = (tmpArray:Array[String])=> getSACACols(tmpArray).flatMap(x => x)

  def nvl(ColIn: org.apache.spark.sql.Column, ReplaceVal: Any) =
    org.apache.spark.sql.functions.when(ColIn.isNull, org.apache.spark.sql.functions.lit(ReplaceVal)).otherwise(ColIn)

  def nvl(ColIn: org.apache.spark.sql.Column, replaceCol: org.apache.spark.sql.Column) =
    org.apache.spark.sql.functions.when(ColIn.isNotNull,ColIn).otherwise(replaceCol)


  def getPassMarkPercentage(examType:String)= examType match {
    case value if value == summativeAssessment => 50
    case value if value == cumulativeAssessment => 45
  }

  def getMaxMarks(examType:String)= examType match {
    case value if value == summativeAssessment => 100.0
    case value if value == cumulativeAssessment => 60.0
  }
  def getBigDecimalFromInt(intValue:Int=0)= new java.math.BigDecimal(intValue)
  def getBigDecimalFromDouble(intValue:Double=0.0)= new java.math.BigDecimal(intValue)

  val getSuccessCheck=(result:Int) => List(0,1).contains(result)
  def getPercentage(maxMarks:java.math.BigDecimal,currentMarks:java.math.BigDecimal)=currentMarks.multiply( new java.math.BigDecimal(100.0).divide(maxMarks,MathContext.DECIMAL128),MathContext.DECIMAL128)

  def getMarksForPercentage(maxMarks:java.math.BigDecimal,percentage:java.math.BigDecimal)=(maxMarks.divide( new java.math.BigDecimal(100.0),MathContext.DECIMAL128) ).multiply(percentage ,MathContext.DECIMAL128)

  def getMaxMarksFinal(examType:String)= examType match {
    case value if value == summativeAssessment => 60
    case value if value == cumulativeAssessment => 40
  }
  def mkString(seqString:Seq[String])=seqString match {case null => "" case _ => seqString.mkString(",")}

  val checkPass:(java.math.BigDecimal,java.math.BigDecimal)=>Boolean =
    (currentMarks:java.math.BigDecimal,passMarks:java.math.BigDecimal) =>  List(0,1).contains(currentMarks.compareTo(passMarks))

  val getReadStreamDF:(org.apache.spark.sql.SparkSession,collection.mutable.Map[String,String])=> org.apache.spark.sql.DataFrame = (spark:org.apache.spark.sql.SparkSession,inputMap:collection.mutable.Map[String,String]) =>
    Try{inputMap(readStreamFormat)} match {
      case Success(s)=>
        println(s"getReadStreamDF :: Success")
        s match {
          case value if value == deltaStreamFormat =>
            println(s"getReadStreamDF :: Success delta")
            Try{inputMap(deltaIgnoreChanges)} match {
              case Success(s) if s.toBoolean == true =>
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

  def saveDFGeneric[T](dfWriter:org.apache.spark.sql.DataFrameWriter[T],path:String)= dfWriter.save(path)

  val mapper=new ObjectMapper() with ScalaObjectMapper
  mapper.registerModule(DefaultScalaModule)
  mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES,false)

  def toJson(value:Any)= mapper.writeValueAsString(value)

  def fromJson[T](json:String)(implicit m:Manifest[T])=mapper.readValue[T](json)

  import org.json4s._
  import org.json4s.jackson.JsonMethods._

  def jsonStrToMap(jsonStr: String)= {
    implicit val formats = org.json4s.DefaultFormats
    parse(jsonStr).extract[Map[String, Any]]
  }





  val simpleDateFormat= new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
  def getTSFromString(tsStr:String)=new java.sql.Timestamp(simpleDateFormat.parse(tsStr).getTime)

  val udfTSFromString= udf(getTSFromString(_:String))
  def innerMsgParser(df:org.apache.spark.sql.DataFrame)=df.select(from_json(col("actualMessage"),innerMarksSchema).as("structExtracted"),udfTSFromString(col("receivingTimeStamp")).as("incomingTS")).select(col("structExtracted.*"),col("incomingTS"))

  def getTs= new java.sql.Timestamp(System.currentTimeMillis)

}




case class kafkaWrapper(value:org.apache.spark.sql.Row)
case class tmpD(cool:String,cool2:String)
object tmpD {
def apply(cool:String,cool2:String):tmpD = tmpD(cool,cool2)
}

import reflect.runtime.universe._

object tmpCode{

  def getRandomMarks(minMarks:Int=1,maxMarks:Int=100)=java.util.concurrent.ThreadLocalRandom.current.nextInt(minMarks,maxMarks)
/*

  val getActualMessage:(Seq[(String,Seq[(String,String)],Seq[String],String,String,Int,Int)]) => Seq[(String,String)] = (dataTuple:Seq[(String,Seq[(String,String)],Seq[String],String,String,Int,Int)]) => (for(data <- dataTuple) yield {
    for (examID <- data._2) yield {
      for (subjectCode <- data._3) yield {
        (s"""{"examId":"${examID._1}","studentID":"${data._1}","subjectCode":"${subjectCode}","revisionNumber":${data._4},"incomingTs":"${data._5}","marks":${getRandomMarks( data._6,data._7)}}""",examID._2)}}}).flatMap(x=>x).flatMap(x=>x)

  val getTS:()=>String= () => new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(java.util.Calendar.getInstance().getTime())

  def getTs=new java.sql.Timestamp(System.currentTimeMillis).toString

  def getKafkaMessage(actualMessages:Seq[( String,Seq[(String,String)],Seq[String],String,String,Int,Int)],receivingTimeStamp:String=getTS())=
    for(actualMessage <- getActualMessage(actualMessages))
      yield {s"""{"messageType":"${actualMessage._2}","actualMessage":"${actualMessage._1.replace("\"","\\\"")}","receivingTimeStamp":"${receivingTimeStamp}"}"""}
*/


  def getActualKafkaMessage(studInfos:Seq[(String,Int,Seq[(String,String)],String,String)])= for( studInfo <- studInfos) yield
    for(kafkaMessage <- getPayload(studInfo).map(_.replace("\"","\\\""))) yield
      s"""{"messageType":"${studInfo._4}","actualMessage":"${kafkaMessage}","receivingTimeStamp":"${getTsCurrentMill()}"}"""


  // examId,revNumber,Seq(subCode),examType,studentId

  def getPayload(studInfo:(String,Int,Seq[(String,String)],String,String))= for(subCodeAndInfo <- studInfo._3) yield
    s"""{"examId":"${studInfo._1}","studentID":"${studInfo._5}","subjectCode":"${subCodeAndInfo._1}","marks":"${studInfo._4 match { case value if value == "SA" =>  subCodeAndInfo._2.toLowerCase match { case "pass" => getRandomMarks(50,100) case "fail"=> getRandomMarks(0,49) case _ =>getRandomMarks()} case "CA" => subCodeAndInfo._2.toLowerCase match { case "pass" => getRandomMarks(Math.ceil((60.0/100)*40.0).toInt,60) case "fail"=> getRandomMarks(0,Math.floor((60.0/100)*40.0).toInt -1) case _ =>getRandomMarks(1,60)}  } }","revisionNumber":${studInfo._2}}"""




  def getTsCurrent = new java.sql.Timestamp(System.currentTimeMillis)
  val getTsCurrentMill :() => java.sql.Timestamp = () => new java.sql.Timestamp(System.currentTimeMillis)


  val getProps:()=> java.util.Properties = () => new java.util.Properties

/*
  def getKafkaProps=getProps() match {case value =>
    value.put("bootstrap.servers","localhost:8081,localhost:8082,localhost:8083")
    value.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer")
    value.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer")
    value
  }

  val getKafkaProducer:() => org.apache.kafka.clients.producer.KafkaProducer[Any,Any] = () => new org.apache.kafka.clients.producer.KafkaProducer(getKafkaProps)

*/

  def getKafkaPropsGeneric[K:TypeTag,V:TypeTag]  = getProps() match {
    case value =>
      value.put("bootstrap.servers","localhost:8081,localhost:8082,localhost:8083")
      value.put("key.serializer", getSerializer[K])
      value.put("value.serializer",getSerializer[V])
      value
  }

  /*
    def getSerializer[T] =  if (Try{0L.asInstanceOf[T]}.isSuccess)
        "org.apache.kafka.common.serialization.LongSerializer"
      else if (Try{0.0D.asInstanceOf[T]}.isSuccess)
        "org.apache.kafka.common.serialization.DoubleSerializer"
      else if (Try{0.0.asInstanceOf[T]}.isSuccess)
        "org.apache.kafka.common.serialization.FloatSerializer"
      else if (Try{0.asInstanceOf[T]}.isSuccess)
        "org.apache.kafka.common.serialization.IntegerSerializer"
      else
        "org.apache.kafka.common.serialization.StringSerializer"
  */
object tmpRun {
    def main (args:Array[String])=
      println(getSerializer[String])
  val baseSerializationPackage="org.apache.kafka.common.serialization"

    def getSerializer[T:TypeTag]= typeOf[T] match {
      case value if value == typeOf[Double] => s"${baseSerializationPackage}.DoubleSerializer"
      case value if value == typeOf[String] =>s"${baseSerializationPackage}.StringSerializer"
      case value if value == typeOf[Float] =>s"${baseSerializationPackage}.FloatSerializer"
      case value if value == typeOf[Int] =>s"${baseSerializationPackage}.IntegerSerializer"
      case value if value == typeOf[Short] =>s"${baseSerializationPackage}.ShortSerializer"
      case value if value == typeOf[Array[Byte]] =>s"${baseSerializationPackage}.ByteArraySerializer"
      case value if value == typeOf[java.sql.Date] =>s"${baseSerializationPackage}.StringSerializer"
      case value if value == typeOf[java.sql.Timestamp] =>s"${baseSerializationPackage}.StringSerializer"
    }
  }



  def getSerializer[T:TypeTag] =
    if(typeOf[Long] == typeOf[T])
      "org.apache.kafka.common.serialization.LongSerializer"
    else if (typeOf[Double] == typeOf[T])
      "org.apache.kafka.common.serialization.DoubleSerializer"
    else if (typeOf[Float] == typeOf[T])
      "org.apache.kafka.common.serialization.FloatSerializer"
    else if (typeOf[Int] == typeOf[T])
      "org.apache.kafka.common.serialization.IntegerSerializer"
    else if (typeOf[Short] == typeOf[T])
      "org.apache.kafka.common.serialization.ShortSerializer"
    else
      "org.apache.kafka.common.serialization.StringSerializer"

/*


  def getLastElemStr(stringToSplit:String,delimiter:String=",")=stringToSplit.split(delimiter).last
  def getLastElemChar(stringToSplit:String,delimiter:Char='.')=stringToSplit.split(delimiter).last

  def getLastElemCharNormalized(stringToSplit:String,delimiter:Char='.')=getLastElemChar(stringToSplit,delimiter).toLowerCase
  def getLastElemStrNormalized(stringToSplit:String,delimiter:String=",")=getLastElemStr(stringToSplit,delimiter).toLowerCase


*/

  def getKafkaProducerGeneric[K:TypeTag,V:TypeTag] = new org.apache.kafka.clients.producer.KafkaProducer[K,V](getKafkaPropsGeneric[K,V])

 // def sendMessage(message:String,topic:String,kafkaProducer:org.apache.kafka.clients.producer.KafkaProducer[Any,Any]=getKafkaProducer())=kafkaProducer.send(new org.apache.kafka.clients.producer.ProducerRecord(topic,getRandomStr(),message))

  def sendMessageGeneric[K:TypeTag,V:TypeTag](key:K,message:V,topic:String)= getKafkaProducerGeneric[K,V] match {
    case kafkaProducer =>
   val recordMeta=kafkaProducer.send(new org.apache.kafka.clients.producer.ProducerRecord(topic,key,message))
      kafkaProducer.close
      recordMeta
  }

  val sendMessages:(Seq[String],String)=>
    Seq[java.util.concurrent.Future[org.apache.kafka.clients.producer.RecordMetadata]]
  = (messages:Seq[String],topic:String) =>for (message <- messages) yield
      sendMessageGeneric[String,String](getRandomStr(),message,topic)

  sendMessages(getActualKafkaMessage(Seq(("e001",1,
    "sub001,sub002,sub003,sub004,sub005".split(",").toSeq.zip(Seq.fill(5)("pass")),"CA","s001")
    ,("e002",1,"sub001,sub002,sub003,sub004,sub005".split(",").toSeq.zip(Seq.tabulate(5)(_ => "pass")),"CA","s001")
    ,("ex001",1,"sub001,sub002,sub003,sub004,sub005".split(",").toSeq.zip((1 to 5).map(_ => "pass")),"SA","s001")))
    .flatMap(x=>x),"topicTmp")

  sendMessages(getActualKafkaMessage(Seq(("e001",3,
    "sub001,sub002,sub003,sub004,sub005".split(",").toSeq.zip(Seq.fill(5)("pass")),"CA","s001")
    ,("e002",4,"sub001,sub002,sub003,sub004,sub005".split(",").toSeq.zip(Seq.tabulate(5)(_ => "pass")),"CA","s001")
    ,("ex001",3,"sub001,sub002,sub003,sub004,sub005".split(",").toSeq.zip((1 to 5).map(_ => "pass")),"SA","s001")))
    .flatMap(x=>x),"topicTmp")

  sendMessages(getActualKafkaMessage(Seq(("e001",3,
    "sub001,sub004".split(",").toSeq.zip(Seq.fill(2)("fail")),"CA","s001")
    ,("e002",4,"sub001,sub002,sub003,sub004,sub005".split(",").toSeq.zip(Seq.tabulate(5)(_ => "pass")),"CA","s001")
    ,("ex001",3,"sub001,sub002,sub003,sub004,sub005".split(",").toSeq.zip((1 to 5).map(_ => "pass")),"SA","s001")))
    .flatMap(x=>x),"topicTmp")

  val chars = (('a' to 'z') ++ ('A' to 'Z')).toSeq
  val charsSize=chars.size

  def arraySizeNormalizer(arrSize:Int)= arrSize-1

  val charsSizeNormalized=arraySizeNormalizer(charsSize)
  val getRandomChar:()=> Char= () =>chars( java.util.concurrent.ThreadLocalRandom.current.nextInt(0,51))

  def getRandomStr(lengthOfString:Int=5,tmpString:String="") :String= lengthOfString match {
    case value if value ==1 => s"${tmpString}${getRandomChar()}"
    case value if value >1 => getRandomStr(value-1,s"${tmpString}${getRandomChar()}")
  }

  object coolTmp {

    def getkVPair(key:String,value:String) =s""""${key}":${value}"""

    def getStrVal(value:String)=s""""${value}""""

    def getRandomMarks(startMark:Int=0,endMark:Int=60) = java.util.concurrent.ThreadLocalRandom.current.nextInt(startMark,endMark)

    def getMarks(marksInfo:(String,String))= marksInfo._1 match {
      case "SA" => marksInfo._2.toLowerCase.startsWith("p") match {
        case true => getRandomMarks(50,100)
        case false => getRandomMarks(0,49)
      }
      case "CA" => marksInfo._2.toLowerCase.startsWith("p") match {
        case true => getRandomMarks(27,60)
        case false => getRandomMarks(0,26)
      }
    }

    val getActualMessage:((String,Seq[(String,Seq[(String,String)],String)]))=> Seq[String] = (studentAndExamInfo:(String,Seq[(String,Seq[(String,String)],String)])) => {for (info  <- studentAndExamInfo._2) yield for (infoSub  <- info._2) yield  Seq(getkVPair("examId",getStrVal(info._1)),getkVPair("studentId",getStrVal(studentAndExamInfo._1)),getkVPair("subjectCode",getStrVal(infoSub._1)),getkVPair("marks", s"${getMarks((info._3,infoSub._2))}"),getkVPair("incomingTs",getStrVal(s"${new java.sql.Timestamp(System.currentTimeMillis)}"))).mkString("{",",","}")}.flatMap(x => x)

    val getPayloadMessage:(String) => String = (actualMessage:String) => actualMessage.replace("\"","\\\"")

    getActualMessage(("s001",Seq(("e001",Seq(("s001","p"), ("s002","p"), ("s003","p"), ("s004","p"), ("s005","p")),"CA"))))

    val getTs:() => String = () => s"""${new java.sql.Timestamp(System.currentTimeMillis)}"""

    val getWrappedMessage:((Seq[String], (String, Seq[(String, String)], String))) => Seq[String] = (messageAndExamInfo:(Seq[String], (String, Seq[(String, String)], String))) => messageAndExamInfo._1.map(x => Seq(getkVPair("messageType",getStrVal(messageAndExamInfo._2._3)),getkVPair("messageType",getStrVal(x)),getkVPair("receivingTimeStamp",getStrVal(getTs()))).mkString("{",",","}"))

    // student id ,exam id,(subjectId's/ [pass / fail]),examType
    def getKafkaMessage(msgInfo:Seq[(String,Seq[(String,Seq[(String,String)],String)])]) = msgInfo.flatMap(x => x._2.map(y => (getActualMessage((x._1,Seq(y))),y))).map(x=> (x._1.map(getPayloadMessage),x._2)).flatMap(getWrappedMessage)

    getKafkaMessage(Seq(("s001",Seq(("e001",Seq(("sub001","p"), ("sub002","p"), ("sub003","p"), ("sub004","p"), ("sub005","p")),"CA"),("e002",Seq(("sub001","p"), ("sub002","p"), ("sub003","p"), ("sub004","p"), ("sub005","p")),"CA"),("ex001",Seq(("sub001","p"), ("sub002","p"), ("sub003","p"), ("sub004","p"), ("sub005","p")),"SA"))),("s002",Seq(("e001",Seq(("sub001","p"), ("sub002","p"), ("sub003","f"), ("sub004","f"), ("sub005","p")),"CA"),("e002",Seq(("sub001","p"), ("sub002","p"), ("sub003","f"), ("sub004","f"), ("sub005","p")),"CA"),("ex001",Seq(("sub001","p"), ("sub002","p"), ("sub003","f"), ("sub004","f")),"SA")))))

    import scala.reflect.runtime.universe._

    def getSerializer[T:TypeTag] = typeOf[T] match {
      case value if  value == typeOf[Long] => "org.apache.kafka.common.serialization.LongSerializer"
      case value if  value == typeOf[String] => "org.apache.kafka.common.serialization.StringSerializer"
      case value if  value == typeOf[Int] =>"org.apache.kafka.common.serialization.IntegerSerializer"
      case value if  value == typeOf[Double] => "org.apache.kafka.common.serialization.DoubleSerializer"
      case value if  value == typeOf[Byte] => "org.apache.kafka.common.serialization.ByteSerializer"
      case value if  value == typeOf[Float] => "org.apache.kafka.common.serialization.FloatSerializer"
    }

    val getProperties :() => java.util.Properties = () => new java.util.Properties

    def getKafkaProps[K:TypeTag,V:TypeTag] = {
      val props=getProperties()
      props.put("key.serializer",getSerializer[K])
      props.put("bootstrap.servers","localhost:8081,localhost:8082,localhost:8083")
      props.put("value.serializer",getSerializer[V])
      props
    }

    def getKafkaProducer[K:TypeTag,V:TypeTag] = new org.apache.kafka.clients.producer.KafkaProducer[K,V](getKafkaProps[K,V])

    def getProducerRecord[K:TypeTag,V:TypeTag](key:K,value:V,topic:String)=
      new org.apache.kafka.clients.producer.ProducerRecord[K,V](topic,key,value)

    import scala.concurrent.Future
    import scala.concurrent.ExecutionContext.Implicits.global

    val sendKafkaMessages:(Seq[String],org.apache.kafka.clients.producer.KafkaProducer[String,String]) => List[Future[org.apache.kafka.clients.producer.RecordMetadata]] = (messages:Seq[String],producer:org.apache.kafka.clients.producer.KafkaProducer[String,String]) => {
      // val producer=getKafkaProducer[String,String]
      // val resT=messages.foldLeft(List(Future(new org.apache.kafka.clients.producer.RecordMetadata(new org.apache.kafka.common.TopicPartition("",0),0L,0L,0L,0L,0,0))))((x,y) => x :+ Future((producer.send(getProducerRecord[String,String]("",y,"tmpTopic"))).get)) // returns java.util.concurrent.Future, we have used sala future
      messages.map(x =>Future((producer.send(getProducerRecord[String,String]("",x,"tmpTopic"))).get)).toList
      // producer.close
      // arrayBuff.toList
    }

    def sendMessages(messages:Seq[String])= getKafkaProducer[String,String] match {
      case value =>
        val result=sendKafkaMessages(messages,value)
        value.close
        result
    }

    def main(args:Array[String])= {
      val producer = getKafkaProducer[String, String]
      sendKafkaMessages(getKafkaMessage(Seq(("s001", Seq(("e001", Seq(("sub001", "p"), ("sub002", "p"), ("sub003", "p"), ("sub004", "p"), ("sub005", "p")), "CA"), ("e002", Seq(("sub001", "p"), ("sub002", "p"), ("sub003", "p"), ("sub004", "p"), ("sub005", "p")), "CA"), ("ex001", Seq(("sub001", "p"), ("sub002", "p"), ("sub003", "p"), ("sub004", "p"), ("sub005", "p")), "SA"))), ("s002", Seq(("e001", Seq(("sub001", "p"), ("sub002", "p"), ("sub003", "f"), ("sub004", "f"), ("sub005", "p")), "CA"), ("e002", Seq(("sub001", "p"), ("sub002", "p"), ("sub003", "f"), ("sub004", "f"), ("sub005", "p")), "CA"), ("ex001", Seq(("sub001", "p"), ("sub002", "p"), ("sub003", "f"), ("sub004", "f")), "SA"))))), producer)
      producer.close
    }

    /*
     ("hdfs dfs -ls hdfs://localhost:8020/user/raptor/persist/marks/"!! ).split("\n").map(_.trim).map(_.split(" ").map(_.trim).filter(_.startsWith("hdfs://"))).flatMap(x=>x).filter(_.size>0).filter(x=> x.split("/").last match { case value if value.toLowerCase.startsWith("ca") ||  value.toLowerCase.startsWith("sa") =>
     true
     case value if value.toLowerCase.startsWith("diamond") || value.toLowerCase.startsWith("gold") => true
     case _ => false
     } ).map(x => s"hdfs dfs -rm -r ${x}"!)

*/
  }

}
