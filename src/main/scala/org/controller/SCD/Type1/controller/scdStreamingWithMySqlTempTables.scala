package org.controller.SCD.Type1.controller

import org.util.SparkOpener
import org.util.readWriteUtil._
import org.constants.projectConstants._
import org.apache.spark.sql.types._
import java.util.Properties
import java.sql.DriverManager

object scdStreamingWithMySqlTempTables extends SparkOpener {
  def main(args: Array[String]): Unit = {
    val inputMap = collection.mutable.Map[String, String]()
    for (arg <- args)
    {
      val keyPart = arg.split("=", 2)(0)
      val valPart = arg.split("=", 2)(1)
      inputMap.put(keyPart, valPart)
    }
    inputMap.put(fileFormatArg, kafkaFormat)
    inputMap.put(checkPointLocationArg, inputMap("checkPointLocationReadStream"))
    inputMap.put(kafkaBootStrapServersArg, inputMap("bootStrapServer"))
    inputMap.put(kafkaTopicArg, inputMap("topic"))
    inputMap.put(kafkaKeyDeserializerArg, kafkaStringDeserializer)
    inputMap.put(kafkaValueDeserializerArg, kafkaStringDeserializer)

    val spark = SparkSessionLoc("tmp")
    import spark.implicits._

    val url="jdbc:mysql://localhost:3306/testSlick"
    val mysqlProps = new Properties()
    mysqlProps.put("url",url)
    mysqlProps.put("user","raptor")
    mysqlProps.put("password","")
    Class.forName("com.mysql.jdbc.Driver")

    val df = readStreamFunction(spark, inputMap).select($"value".cast(StringType)).selectExpr("split(value,'~') as valueSplitted").selectExpr("valueSplitted[0] as ADDRESS_ID", "valueSplitted[1] as ADDRESS_LINE", "valueSplitted[2] as CITY", "valueSplitted[3] as POSTAL_CODE", "valueSplitted[4] as USER_ID")

    df.writeStream.outputMode("update").option("checkpointLocation","/user/raptor/kafka/stream/mysqlUpsertCheckointSQL/").foreachBatch{
      (dfTemp:org.apache.spark.sql.DataFrame,batchNum:Long) =>
        println(s"Batch ${batchNum}")
        val mysqlConnection = DriverManager.getConnection(url, mysqlProps)

        //scd 1
        dfTemp.write.mode("overwrite").format("jdbc").option("driver","com.mysql.jdbc.Driver").option("url","jdbc:mysql://localhost:3306/testSlick").option("user","raptor").option("password","").option("dbtable","testSlick.address_temp").save

        // use prepared statement

        //val updatedRecordsDF=spark.read.format("jdbc").option("driver","com.mysql.jdbc.Driver").option("url","jdbc:mysql://localhost:3306/testSlick").option("user","raptor").option("password","").option("dbTable","(create table testSlick.address_temp_updated as select * from (select base.ADDRESS_ID ,coalesce(batch.ADDRESS_LINE,base.ADDRESS_LINE) ADDRESS_LINE ,coalesce(batch.CITY,base.CITY) CITY ,coalesce(batch.POSTAL_CODE,base.POSTAL_CODE) POSTAL_CODE , coalesce(batch.USER_ID,base.USER_ID) USER_ID from testSlick.address base left join testSlick.address_temp batch on base.ADDRESS_ID=batch.ADDRESS_ID where batch.ADDRESS_ID is not null) a) t").load

        val updatedTempAddressQuery="create table testSlick.address_temp_updated as select * from (select base.ADDRESS_ID ,coalesce(batch.ADDRESS_LINE,base.ADDRESS_LINE) ADDRESS_LINE ,coalesce(batch.CITY,base.CITY) CITY ,coalesce(batch.POSTAL_CODE,base.POSTAL_CODE) POSTAL_CODE , coalesce(batch.USER_ID,base.USER_ID) USER_ID from testSlick.address base left join testSlick.address_temp batch on base.ADDRESS_ID=batch.ADDRESS_ID where batch.ADDRESS_ID is not null) a"

        val updatedTempAddressQueryStatement=mysqlConnection.prepareStatement(updatedTempAddressQuery)
        println("Creating updated records table")
        updatedTempAddressQueryStatement.executeUpdate()
        println("Created updated records table")
        //val newRecordsDF=spark.read.format("jdbc").option("driver","com.mysql.jdbc.Driver").option("url","jdbc:mysql://localhost:3306/testSlick").option("user","raptor").option("password","").option("dbTable","(select batch.* from testSlick.address base right join testSlick.address_temp batch on base.ADDRESS_ID=batch.ADDRESS_ID where base.ADDRESS_ID is null) a").load

        val newTempAddressQuery="create table testSlick.address_temp_new as select * from (select batch.* from testSlick.address base right join testSlick.address_temp batch on base.ADDRESS_ID=batch.ADDRESS_ID where base.ADDRESS_ID is null) a"
        val newTempAddressQueryStatement=mysqlConnection.prepareStatement(newTempAddressQuery)
        println("Creating new records table")
        newTempAddressQueryStatement.executeUpdate()
        println("Created new records table")

        //val batchNum=3

        //val backupOld=spark.read.format("jdbc").option("driver","com.mysql.jdbc.Driver").option("url","jdbc:mysql://localhost:3306/testSlick").option("user","raptor").option("password","").option("dbTable",s"(select base.*, current_timestamp() as backupTimeStamp,${batchNum} as batchNumber from testSlick.address base left join testSlick.address_temp batch on base.ADDRESS_ID=batch.ADDRESS_ID where batch.ADDRESS_ID is not null) a").load

        val backupOldAddressQuery=s"insert into testSlick.address_temp_backup select * from (select base.*, current_timestamp() as backupTimeStamp,${batchNum} as batchNumber from testSlick.address base left join testSlick.address_temp batch on base.ADDRESS_ID=batch.ADDRESS_ID where batch.ADDRESS_ID is not null) a"
        val backupOldAddressQueryStatement=mysqlConnection.prepareStatement(backupOldAddressQuery)
        println("Taking backup of old records table")
        backupOldAddressQueryStatement.executeUpdate()
        println("Took backup of old records table")

        //val deletingFromBaseTable=spark.read.format("jdbc").option("driver","com.mysql.jdbc.Driver").option("url","jdbc:mysql://localhost:3306/testSlick").option("user","raptor").option("password","").option("dbTable","(delete from testSlick.address where ADDRESS_ID in (select distinct ADDRESS_ID from testSlick.address_temp) ) a").load

        val deleteOldAddressQuery="delete from testSlick.address where ADDRESS_ID in (select distinct ADDRESS_ID from testSlick.address_temp)"
        val deleteOldAddressQueryStatement=mysqlConnection.prepareStatement(deleteOldAddressQuery)
        println("Deleting old records table")
        deleteOldAddressQueryStatement.executeUpdate()
        println("Deleted old records table")

        val updatedRecordsDF=spark.read.format("jdbc").option("driver","com.mysql.jdbc.Driver").option("url","jdbc:mysql://localhost:3306/testSlick").option("user","raptor").option("password","").option("dbTable","testSlick.address_temp_updated").load

        val newRecordsDF=spark.read.format("jdbc").option("driver","com.mysql.jdbc.Driver").option("url","jdbc:mysql://localhost:3306/testSlick").option("user","raptor").option("password","").option("dbTable","testSlick.address_temp_new").load

        //inserting new records --------------------
        val recordsToBeInserted=newRecordsDF.union(updatedRecordsDF)

        recordsToBeInserted.write.mode("append").format("jdbc").option("driver","com.mysql.jdbc.Driver").option("url","jdbc:mysql://localhost:3306/testSlick").option("user","raptor").option("password","").option("dbTable","testSlick.address").save

        // deleting temporary tables
        val deleteNewTempTableQuery="drop table if exists testSlick.address_temp_new"
        val deleteNewTempTableQueryStatement=mysqlConnection.prepareStatement(deleteNewTempTableQuery)
        println("Deleting new records temporary table")
        deleteNewTempTableQueryStatement.executeUpdate()
        println("Deleted new records temporary table")

        val deleteUpdatedTempTableQuery="drop table if exists testSlick.address_temp_updated"
        val deleteUpdatedTempTableQueryStatement=mysqlConnection.prepareStatement(deleteUpdatedTempTableQuery)
        println("Deleting updated records temporary table")
        deleteUpdatedTempTableQueryStatement.executeUpdate()
        println("Deleted updated records temporary table")

    }.start.awaitTermination
  }
}
