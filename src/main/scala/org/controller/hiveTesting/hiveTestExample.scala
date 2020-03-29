package org.controller.hiveTesting

import org.util.readWriteUtil._
import org.apache.spark.sql.SparkSession

object hiveTestExample {
  def main(args: Array[String]):Unit={
    val inputMap:collection.mutable.Map[String,String]= collection.mutable.Map[String,String]()
    for(arg <- args)
      {
        val keyPart=arg.split("=",2)(0)
        val valPart=arg.split("=",2)(1)
        inputMap.put(keyPart,valPart)
      }
     //  val warehouseLocation="/user/raptor/tmp/hive/warehouse/"
    val warehouseLocation=inputMap("warehouseLocation")
    val hiveUri=inputMap("hiveUri")
    val db=inputMap("db")
    val table=inputMap("table")

    val spark = SparkSession.builder().appName("Spark Hive Example").config("hive.metastore.uris",hiveUri).config("spark.sql.warehouse.dir", warehouseLocation).enableHiveSupport().getOrCreate()

    val sql=s"select * from ${db}.${table}"
    val sqlTest=execSparkSql(spark,sql)
    sqlTest.show

    //      .config("hive.metastore.uris","thrift://localhost:3306/metastore")
    // .config("spark.sql.catalogImplementation","hive")
//?createDatabaseIfNotExist=true?username=HIVE_USER?password=HIVE_PASSWORD
  }

}
