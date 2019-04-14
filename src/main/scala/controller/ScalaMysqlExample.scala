package controller

import util.SparkOpener

object ScalaMysqlExample extends SparkOpener {

  val spark=SparkSessionLoc("SparkSession")

  def main(args: Array[String]): Unit =
  {
    val Temp_tab_DF=spark.read.format("jdbc").option("url","jdbc:mysql://localhost:3306/?user=root&password=").option("dbtable","vsms.branch").load()
     // .option("partitionColumn","VEHICLES_SOLD").option("lowerBound","23").option("upperBound","25").option("numPartitions","3")  - will work if you really read a partitioned table
    Temp_tab_DF.show()
    Temp_tab_DF.printSchema()
    spark.close()
  }
}
