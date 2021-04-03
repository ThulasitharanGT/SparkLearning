package org.controller.persistingInsideJob

import org.apache.spark.sql.types.{StringType, StructField, StructType}

object jobConstantsPersist {
  val schemaOfMessage=StructType(Array(StructField("eventInfo",StringType,true),StructField("eventData",StringType,true)))
  val teamEvent="teamEvent"
  val driverEvent="driverEvent"


}
