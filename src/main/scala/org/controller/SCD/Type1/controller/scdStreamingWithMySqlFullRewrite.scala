package org.controller.SCD.Type1.controller

import org.util.SparkOpener
import org.util.readWriteUtil._
import org.constants.projectConstants._

import org.apache.spark.sql.types._

object scdStreamingWithMySqlFullRewrite extends SparkOpener{

  def main (args:Array[String]):Unit ={

    val inputMap=collection.mutable.Map[String,String]()

    for (arg <- args)
      {
        val keyPart=arg.split("=",2)(0)
        val valPart=arg.split("=",2)(1)
        inputMap.put(keyPart,valPart)
      }





    val spark=SparkSessionLoc("tmp")
    import spark.implicits._

    //val df=spark.readStream.format("kafka")/*.option("failOnDataLoss","false")*/.option("kafka.bootstrap.servers","localhost:9092,localhost:9093.localhost:9094").option("subscribe","tempUpdate").option("offset","earliest").option("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer").option("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer").load.select($"value".cast(StringType)).selectExpr("split(value,'~') as valueSplitted").selectExpr("valueSplitted[0] as ADDRESS_ID","valueSplitted[1] as ADDRESS_LINE","valueSplitted[2] as CITY","valueSplitted[3] as POSTAL_CODE","valueSplitted[4] as USER_ID")

    inputMap.put(fileFormatArg,kafkaFormat)
    inputMap.put(checkPointLocationArg,inputMap("checkPointLocationReadStream"))
    inputMap.put(kafkaBootStrapServersArg,inputMap("bootStrapServer"))
    inputMap.put(kafkaTopicArg,inputMap("topic"))
    inputMap.put(kafkaKeyDeserializerArg,kafkaStringDeserializer)
    inputMap.put(kafkaValueDeserializerArg,kafkaStringDeserializer)

    val df=readStreamFunction(spark,inputMap).select($"value".cast(StringType)).selectExpr("split(value,'~') as valueSplitted").selectExpr("valueSplitted[0] as ADDRESS_ID","valueSplitted[1] as ADDRESS_LINE","valueSplitted[2] as CITY","valueSplitted[3] as POSTAL_CODE","valueSplitted[4] as USER_ID")



    df.writeStream.outputMode("update").option("failOnDataLoss","false").option("checkpointLocation","/user/raptor/kafka/stream/mysqlUpsertCheckoint/").foreachBatch{
      (dfTemp:org.apache.spark.sql.DataFrame,batchNum:Long) =>
        println(s"Batch - ${batchNum}")

        println(s"Full table data")

        val fullTableDF=spark.read.format("jdbc").option("driver","com.mysql.jdbc.Driver").option("url","jdbc:mysql://localhost:3306/testSlick").option("user","raptor").option("password","").option("dbtable","testSlick.address").load

        fullTableDF.show(false)

        println("Records in this batch")

        dfTemp.show(false)

        println(s"Records to be updated")

        val recordsToBeUpdated=fullTableDF.as("full").join(dfTemp.as("batch"),$"full.ADDRESS_ID" === $"batch.ADDRESS_ID" ,"left").filter("batch.ADDRESS_ID is not NULL").selectExpr("full.ADDRESS_ID","nvl(batch.ADDRESS_LINE,full.ADDRESS_LINE) ADDRESS_LINE","nvl(batch.CITY,full.CITY) CITY","nvl(batch.POSTAL_CODE,full.POSTAL_CODE) POSTAL_CODE","nvl(batch.USER_ID,full.USER_ID) USER_ID") //Seq("ADDRESS_ID") , the updated value is taken and other values with null fields being sent are kept

        recordsToBeUpdated.show(false)

        println(s"Records to be retained as it is")

        val recordsWhichDidNotComeIn=fullTableDF.as("full").join(dfTemp.as("batch"),$"full.ADDRESS_ID" === $"batch.ADDRESS_ID" ,"left").filter("batch.ADDRESS_ID is NULL").selectExpr("full.*")

        recordsWhichDidNotComeIn.show(false)

        println(s"Records completely new")

        val recordsCompletelyNew=fullTableDF.as("full").join(dfTemp.as("batch"),$"full.ADDRESS_ID" === $"batch.ADDRESS_ID" ,"right").filter("full.ADDRESS_ID is NULL").selectExpr("batch.*")

        recordsCompletelyNew.show(false)

        println(s"Entire new data ")

        val entieTableData=recordsToBeUpdated.union(recordsWhichDidNotComeIn).union(recordsCompletelyNew)

        entieTableData.show(false)

        println(s"Wriring to table ")

        entieTableData.write.format("jdbc").mode("overwrite").option("url","jdbc:mysql://localhost:3306/testSlick").option("user","raptor").option("password","").option("dbtable","testSlick.address").save()

    }.start.awaitTermination
  }
}
