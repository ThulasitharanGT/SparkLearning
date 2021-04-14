package org.controller.persistingInsideJob

import org.apache.spark.sql.types.{DateType, StringType, StructField, StructType}

object jobConstantsPersist {
  val schemaOfMessage=StructType(Array(StructField("eventInfo",StringType,true),StructField("eventData",StringType,true)))
  val teamEvent="teamEvent"
  val driverEvent="driverEvent"
  val driverStatsEvent="driverStatsEvent"
  val driverSchema=StructType(Array(StructField("teamId",StringType,true),StructField("driverId",StringType,true),StructField("driverName",StringType,true),StructField("activeFlag",StringType,true)))
  val teamSchema=StructType(Array(StructField("teamName",StringType,true),StructField("teamId",StringType,true),StructField("checkFlag",StringType,true)))
  val driverStatsSchema=StructType(Array(StructField("driverName",StringType,true),StructField("driverId",StringType,true),StructField("totalPoles",StringType,true),StructField("totalWins",StringType,true),StructField("totalLapsLead",StringType,true),StructField("recordedDate",DateType,true)))
}
