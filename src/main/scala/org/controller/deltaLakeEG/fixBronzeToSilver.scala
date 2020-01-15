package org.controller.deltaLakeEG

import io.delta.tables._
import org.util.{SparkOpener, readWriteUtil}
import org.apache.spark.sql.functions._
object fixBronzeToSilver extends SparkOpener {
  val spark=SparkSessionLoc("fix bronze to silver")
  def main(args:Array[String]):Unit={
    val inputMap:collection.mutable.Map[String,String]=collection.mutable.Map[String,String]()
    for (arg <- args)
    {
      val argKey=arg.split("=",2)(0)
      val argValue=arg.split("=",2)(1)
      inputMap.put(argKey,argValue)
    }
    val modeForDeltaWrite=inputMap("mode")
    val outputBasePath=inputMap("basePath")+"outputFiles/"
    val deltaTableBaseName=inputMap("deltaTableBaseName")
    val deltaTableBronze=DeltaTable.forPath(spark,outputBasePath+deltaTableBaseName+"_Bronze")
    val deltaTableSilver=DeltaTable.forPath(spark,outputBasePath+deltaTableBaseName+"_Silver")
    deltaTableBronze.toDF.where("brand in ('Lambhorghini','Jeep','Hyundai','Konegzegg','Pagani','Suzuki','BAC','Ford','Bugatti')").drop("number_of_owners").withColumn("number_of_owners",when(col("brand") === "Lambhorghini",4).when(col("brand") === "Konegzegg",3).when(col("brand") === "Pagani",2).when(col("brand") === "Bugatti",1).when(col("brand") === "Suzuki",10).when(col("brand") === "Ford",5).otherwise(15)  ).write.mode(modeForDeltaWrite).option("spark.sql.sources.partitionOverwriteMode","dynamic").format("delta").partitionBy ("brand", "model", "year", "month").save(outputBasePath+deltaTableBaseName+"_Silver")
    println("---------------------------------------SILVER--------------------------------------------------")
    deltaTableSilver.toDF.select("brand","number_of_owners").distinct.show
    deltaTableSilver.toDF.groupBy("brand","model","year").agg(countDistinct("Vehicle_id").as("distinct_Vehicle_id")).show
    println("---------------------------------------BRONZE--------------------------------------------------")
    deltaTableBronze.toDF.select("brand","number_of_owners").distinct.show
    deltaTableBronze.toDF.groupBy("brand","model","year").agg(countDistinct("Vehicle_id").as("distinct_Vehicle_id")).show

  }

}
