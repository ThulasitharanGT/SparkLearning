package org.controller.deltaLakeEG
import io.delta.tables._
import org.util.SparkOpener

object deltaLakeHadoopEg extends SparkOpener {
val spark=SparkSessionLoc("delta lake hadoop")
  def main(args: Array[String]): Unit = {
    val deltaTableInput1=spark.read.format("com.databricks.spark.csv").option("header","true").option("delimiter","|").option("inferSchema","true").load("hdfs://localhost/user/raptor/testing/hadoop/deltaTableTestFolder/inputFiles/Avail_car3.txt")
    //val deltaTableInput2=spark.read.format("com.databricks.spark.csv").option("header","true").option("delimiter","|").option("inferSchema","true").load("hdfs://localhost/user/raptor/testing/hadoop/deltaTableTestFolder/inputFiles/Avail_car2.txt")
   // val deltaTableInput3=spark.read.format("com.databricks.spark.csv").option("header","true").option("delimiter","|").option("inferSchema","true").load("hdfs://localhost/user/raptor/testing/hadoop/deltaTableTestFolder/inputFiles/Avail_car4.txt")
    val tempDeltaTableInput1= deltaTableInput1.selectExpr("Vehicle_id","model","brand","year","month","miles","CAST(concat(substring(intake_date_time,7,4),concat(substring(intake_date_time,3,4),concat(substring(intake_date_time,1,2),substring(intake_date_time,11,9)))) AS TIMESTAMP) as intake_date_time")
   // deltaTableInput2.write.mode("overwrite").option("spark.sql.sources.partitionOverwriteMode","dynamic").format("delta").save("hdfs://localhost/user/raptor/testing/hadoop/deltaTableTestFolder/temp_DeltaTablePartitioned")
    tempDeltaTableInput1.write.mode("overwrite").partitionBy("brand","model","year","month").format("delta").save("hdfs://localhost/user/raptor/testing/hadoop/deltaTableTestFolder/temp_DeltaTablePartitioned")
    val deltaTableDeltaFormat=DeltaTable.forPath(spark,"/user/raptor/testing/hadoop/deltaTableTestFolder/temp_DeltaTablePartitioned")
    //deltaTableInput3.write.mode("append").option("spark.sql.sources.partitionOverwriteMode","dynamic").format("delta").save("hdfs://localhost/user/raptor/testing/hadoop/deltaTableTestFolder/temp_DeltaTablePartitioned")
   // deltaTableDeltaFormat.updateExpr("model = 'Eco-Sport'",Map("model"->"'Free-Style'","year"->"2017")) // string in single quoutes in update expr
    deltaTableDeltaFormat.as("HIST").merge(deltaTableInput1.as("CURR"),"HIST.Vehicle_id = CURR.Vehicle_id").whenMatched().updateAll().whenNotMatched().insertAll().execute()

  }
}
