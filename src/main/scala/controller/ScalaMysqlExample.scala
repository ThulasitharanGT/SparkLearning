package controller

import java.util.Properties

import util.SparkOpener

object ScalaMysqlExample extends SparkOpener {

  val spark=SparkSessionLoc("SparkSession")

  def main(args: Array[String]): Unit =
  {
    val Temp_tab_DF=spark.read.format("jdbc").option("url","jdbc:mysql://localhost:3306/?user=root&password=").option("dbtable","vsms.customer").load()
     // .option("partitionColumn","VEHICLES_SOLD").option("lowerBound","23").option("upperBound","25").option("numPartitions","3")  - will work if you really read a partitioned table
    Temp_tab_DF.show()
    Temp_tab_DF.printSchema()
   // Works onlyy on dfs and hive metastore
   // val Temp_tab_Schema= spark.read.format("jdbc").option("url","jdbc:mysql://localhost:3306/vsms?user=root&password=").table("vsms.staff").schema
    // println(Temp_tab_Schema)
    val url="jdbc:mysql://localhost:3306/"
    val properties = new Properties()
    properties.put("user","root")
    properties.put("password","")
    Class.forName("com.mysql.jdbc.Driver")
    val Revenue_df=spark.read.format("csv").option("header","true").option("delimiter","|").option("inferSchema","true").load("D:\\study\\productrevenue.txt")
    Revenue_df.write.mode("append").jdbc(url,"vsms.temp",properties )

    spark.close()
  }
}
