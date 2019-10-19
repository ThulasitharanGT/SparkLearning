package org.constants

object projectConstants {
  val delimiterArgConstant="delimiter"
  val inferSchemaArgConstant="inferSchema"
  val csvFormat="com.databricks.spark.csv"
  val fileFormatArg="format"
  val columnNameArg="columnNames"
  val columnNameSepArg="columnNameSeparator"
  val fileTypeArgConstant="fileType"
  val fileTypeParquetValue="parquet"
  val fileTypeAvroValue="avro"
  val fileTypeOrcValue="orc"
  val fileTypeJsonValue="json"
  val fileTypeXmlValue="xml"
  val fileRootTagXmlArg ="rootTag"
  val fileRowTagXmlArg ="rowTag"
  val fileTypeXmlFormatValue="com.databricks.spark.xml"
  val fileTypeCsvValue="csvHeader"
  val fileTypeCsvHeaderColumnPassedValue="csvNoHeaderColumnPassed"
  val filePathArgValue="inputPath"
  val headerArgConstant="header"
  val emptyValueArg="emptyValue"
  val fileOverwriteValue="overwrite"
  val fileAppendValue="append"
  val fileOverwriteAppendArg="fileOverwriteAppendArg"
  val delimiterOr="|"
  val delimiterNot="!"
  val delimiterComma=","
  val booleanTrue=true
  val booleanFalse=false
  val stringTrue="true"
  val stringFalse="false"
  val basePathArgConstant="basePath"
  val basePathValueConstant="basePath"
  def trueFalseFinder(value:String)={
    value  match {case value if value==stringTrue => projectConstants.booleanTrue ; case value  if value== stringFalse  => projectConstants.booleanFalse ; case _ => projectConstants.booleanFalse }
  }
}

