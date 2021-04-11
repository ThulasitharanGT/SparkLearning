package org.controller.persistingInsideJob

import org.apache.spark.sql.types.{StringType, StructField, StructType}

object jobConstantsPersist {
  val schemaOfMessage=StructType(Array(StructField("eventInfo",StringType,true),StructField("eventData",StringType,true)))
  val teamEvent="teamEvent"
  val driverEvent="driverEvent"
  val driverSchema=StructType(Array(StructField("teamId",StringType,true),StructField("driverId",StringType,true),StructField("driverName",StringType,true),StructField("activeFlag",StringType,true)))
  val teamSchema=StructType(Array(StructField("teamName",StringType,true),StructField("teamId",StringType,true),StructField("checkFlag",StringType,true)))


}
