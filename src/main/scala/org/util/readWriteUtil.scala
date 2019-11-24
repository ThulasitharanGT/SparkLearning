package org.util
import org.apache.spark.sql.DataFrame

import org.apache.spark.sql.SparkSession
import org.constants.projectConstants
import io.delta.tables._
object readWriteUtil {
  def readDF(spark:SparkSession,inputMap:collection.mutable.Map[String,String]):DataFrame ={
     var dfTemp=spark.createDataFrame(Seq(("invalid Scn"," Error"))).toDF("Scn","Desc")
    inputMap(projectConstants.fileTypeArgConstant)  match
      {
      case value if value==projectConstants.fileTypeParquetValue =>dfTemp= spark.read.option(projectConstants.basePathArgConstant,inputMap(projectConstants.basePathValueConstant)).load(inputMap(projectConstants.filePathArgValue))
      case value if value== projectConstants.fileTypeCsvValue =>dfTemp= spark.read.format(inputMap(projectConstants.fileFormatArg)).option(projectConstants.delimiterArgConstant,inputMap(projectConstants.delimiterArgConstant)).option(projectConstants.headerArgConstant,inputMap(projectConstants.headerArgConstant)).option(projectConstants.inferSchemaArgConstant,inputMap(projectConstants.inferSchemaArgConstant)).option(projectConstants.basePathArgConstant,inputMap(projectConstants.basePathValueConstant)).load(inputMap(projectConstants.filePathArgValue))
      case value if value==projectConstants.fileTypeCsvHeaderColumnPassedValue =>
        {
        inputMap.put(projectConstants.fileTypeArgConstant,projectConstants.fileTypeCsvValue)
        inputMap.put(projectConstants.headerArgConstant,projectConstants.stringFalse)
        val  df = readDF(spark,inputMap)
        val columnNames=inputMap(projectConstants.columnNameArg)
        val columnNameArray:Array[String]=columnNames.split(inputMap(projectConstants.columnNameSepArg))
        val columnNameSeq=columnNameArray.toSeq
        dfTemp=df.toDF(columnNameSeq:_*)
        }
      case value if value== projectConstants.fileTypeJsonValue => dfTemp= spark.read.json(inputMap(projectConstants.filePathArgValue))
      case value if value== projectConstants.fileTypeAvroValue => dfTemp= spark.read.format(projectConstants.fileTypeAvroFormatValue).load(inputMap(projectConstants.filePathArgValue))
      case value if value== projectConstants.fileTypeOrcValue => dfTemp= spark.read.format(value).option(projectConstants.inferSchemaArgConstant,inputMap(projectConstants.inferSchemaArgConstant)).load(inputMap(projectConstants.filePathArgValue))
      case value if value== projectConstants.fileTypeXmlValue => dfTemp= spark.read.format(projectConstants.fileTypeXmlFormatValue).option(projectConstants.fileRootTagXmlArg,inputMap(projectConstants.fileRootTagXmlArg)).option(projectConstants.fileRowTagXmlArg,inputMap(projectConstants.fileRowTagXmlArg)).load(inputMap(projectConstants.filePathArgValue))
      case value if value== projectConstants.fileTypeDeltaValue => dfTemp= spark.read.format(projectConstants.fileTypeDeltaValue).load(inputMap(projectConstants.filePathArgValue))
      case _ => println("Invalid selection")
      }
    dfTemp
  }

  def deltaTableRead(spark:SparkSession,inputMap:collection.mutable.Map[String,String]) ={
    DeltaTable.forPath(spark,inputMap(projectConstants.filePathArgValue))
  }

  def execSparkSql(spark:SparkSession,sql:String):DataFrame={
    spark.sql(sql)
  }

  def writeDF(inputMap:collection.mutable.Map[String,String],df:DataFrame)={
    inputMap(projectConstants.fileTypeArgConstant)  match
      {
      case value if value==projectConstants.fileTypeParquetValue =>df.write.mode(inputMap(projectConstants.fileOverwriteAppendArg)).option(projectConstants.emptyValueArg,inputMap(projectConstants.emptyValueArg)).parquet(inputMap(projectConstants.filePathArgValue))
      case value if value== projectConstants.fileTypeCsvValue =>df.write.mode(inputMap(projectConstants.fileOverwriteAppendArg)).option(projectConstants.delimiterArgConstant,inputMap(projectConstants.delimiterArgConstant)).option(projectConstants.headerArgConstant,inputMap(projectConstants.headerArgConstant)).csv(inputMap(projectConstants.filePathArgValue))
      case value if value== projectConstants.fileTypeAvroValue =>df.write.mode(inputMap(projectConstants.fileOverwriteAppendArg)).format(projectConstants.fileTypeAvroValue).save(inputMap(projectConstants.filePathArgValue))
      case value if value== projectConstants.fileTypeJsonValue =>df.write.mode(inputMap(projectConstants.fileOverwriteAppendArg)).json(inputMap(projectConstants.filePathArgValue))
      case value if value== projectConstants.fileTypeXmlValue =>df.write.mode(inputMap(projectConstants.fileOverwriteAppendArg)).format(projectConstants.fileTypeXmlFormatValue).option(projectConstants.fileRowTagXmlArg,inputMap(projectConstants.fileRowTagXmlArg)).option(projectConstants.fileRootTagXmlArg,inputMap(projectConstants.fileRootTagXmlArg)).save(inputMap(projectConstants.filePathArgValue))
      case value if value== projectConstants.fileTypeOrcValue =>df.write.format( projectConstants.fileTypeOrcValue ).mode(inputMap(projectConstants.fileOverwriteAppendArg)).save(inputMap(projectConstants.filePathArgValue))
      case value if value== projectConstants.fileTypeDeltaValue =>df.write.format( projectConstants.fileTypeDeltaValue ).mode(inputMap(projectConstants.fileOverwriteAppendArg)).save(inputMap(projectConstants.filePathArgValue))

      //default TextFile
      case _=>df.write.mode(inputMap(projectConstants.fileOverwriteAppendArg)).option(projectConstants.delimiterArgConstant,inputMap(projectConstants.delimiterArgConstant)).option(projectConstants.headerArgConstant,inputMap(projectConstants.headerArgConstant)).text(inputMap(projectConstants.filePathArgValue))
    }
  }
}

/*df.coalesce(1).write.mode("overwrite").format("csv")
    .option("delimiter", ",")
    .option("nullValue", "unknown")
    .option("treatEmptyValuesAsNulls", "false")
    .save(s"$path/test")*/