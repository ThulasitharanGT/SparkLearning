package org.controller.rankingSystemF1

import org.controller.rankingSystemF1.utils._
import org.util.SparkOpener
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object pointsRecalculation extends SparkOpener{
  val spark=SparkSessionLoc()
  spark.sparkContext.setLogLevel("ERROR")
  def main(args:Array[String]):Unit={
    val inputMap=getInputMap(args)

    spark.readStream.format("kafka").option("kafka.bootstrap.servers",inputMap("bootStrapServer"))
      .option("subscribe",inputMap("topic"))
      .option("startingOffsets",inputMap("startingOffsets")).load
      .select(from_json(col("value").cast(StringType).as("kafkaMsg"),schemaOfOuterLayer).as("extractedOuter"))
      .select("extractedOuter.*").filter("messageType='pointsRecalculation'")
      .select(from_json(col("incomingMessage"),schemaOfPointsRecalculation).as("extractedMessage")
        ,col("messageTimestamp"))
      .select("extractedMessage.*","messageTimestamp").writeStream.format("console").outputMode("append")
      .option("checkpointLocation",inputMap("checkPointLocation"))
      .foreachBatch((df:org.apache.spark.sql.DataFrame,batchId:Long) => {
        forEachBatchFun(df,batchId,inputMap)
      }).start

    spark.streams.awaitAnyTermination

  }

  def forEachBatchFun(df:org.apache.spark.sql.DataFrame,batchID:Long,inputMap:collection.mutable.Map[String,String])={

    val seasonsToRecalculate=df.select("season").distinct.collect.map(_(0).toString.toInt) // (org.apache.spark.sql.Encoders.bean[pointsReCalc])

    recalculateTheseSeasons(seasonsToRecalculate,inputMap)

  }
  def recalculateTheseSeasons(seasons:Array[Int],inputMap:collection.mutable.Map[String,String])=  for (season<- seasons)
    writeToStandingsTable(recalculateThisSeason(season,inputMap),season,inputMap)

  def recalculateThisSeason(season:Int,inputMap:collection.mutable.Map[String,String])={
    val driverPositionsDF=spark.read.format("jdbc")
      .option("driver",inputMap("JDBCDriver"))
      .option("user",inputMap("JDBCUser"))
      .option("password",inputMap("JDBCPassword"))
      .option("url",inputMap("JDBCUrl"))
      .option("dbtable",s"""(select * from ${inputMap("schemaName")}.${inputMap("driverPointsTable")} where season =${season} )a""").load.drop("point")

    val pointsDF=spark.read.format("jdbc")
      .option("driver",inputMap("JDBCDriver"))
      .option("user",inputMap("JDBCUser"))
      .option("password",inputMap("JDBCPassword"))
      .option("url",inputMap("JDBCUrl"))
      .option("dbtable",s"""(select * from ${inputMap("schemaName")}.${inputMap("pointsPositionMapTable")} where season =${season} and end_date is null)a""")
      .load

    val pointsAndPositionDF=driverPositionsDF.join(pointsDF,Seq("position"),"left").na.fill(0,Seq("point"))
      .select("driver_id,race_id,point,position".split(",").toSeq.map(col):_*)

    val nonZeroPositionCalculationDF=pointsAndPositionDF
      .withColumn("totalPoints",sum("point").over(Window.partitionBy("driver_id")))
      .filter("totalPoints !=0")
      .withColumn("totalPositions",sum("position").over(Window.partitionBy("driver_id")))
      .withColumn("totalRaces",count("race_id").over(Window.partitionBy("driver_id")))
    .withColumn("latestRecordPerDriver",row_number.over(Window.partitionBy("driver_id").orderBy(desc("totalPoints"))))
      .filter("latestRecordPerDriver=1").drop("latestRecordPerDriver")
      .withColumn("positionVsRacePercentage",col("totalPositions")/col("totalRaces"))
      .withColumn("driverStandings",
        row_number.over(Window.orderBy(desc("totalPoints"),asc("positionVsRacePercentage"))))
      .select("driver_id","totalPoints","driverStandings","positionVsRacePercentage")


    val zeroPositionCalculationDF=pointsAndPositionDF.groupBy("driver_id")
      .agg(sum("point").as("totalPoints")
        ,count("race_id").as("totalRaces")
        ,sum("position").as("totalPositions")).where("totalPoints=0")
      .withColumn("positionVsRacePercentage",col("totalPositions")/col("totalRaces"))
      .withColumn("driverStandings",
        row_number.over(Window.partitionBy("totalPoints").orderBy(asc("positionVsRacePercentage"))))
      .select("driver_id","totalPoints","driverStandings","positionVsRacePercentage")

    val finalTotalPoints=nonZeroPositionCalculationDF.union(zeroPositionCalculationDF)
      .withColumn("maxRaceNonZeroPosition",max("driverStandings").over(Window.orderBy("driver_id")))
      .withColumn("driverStandings",when(col("totalPoints")===lit(0)
        ,col("driverStandings")+col("maxRaceNonZeroPosition")).otherwise(col("driverStandings")))
      .drop("maxRaceNonZeroPosition")

 //   finalTotalPoints.show(false)

    finalTotalPoints
  }

  def deleteSeasonInfoInStandingsTable(season:Int,inputMap:collection.mutable.Map[String,String])={
   val conn=getJDBCConnection(inputMap)
    conn.prepareStatement(s"delete from ${inputMap("schemaName")}.${inputMap("driverStandingsTable")} where season=${season}")
    conn.close
  }
  def writeToStandingsTable(df:org.apache.spark.sql.DataFrame,season:Int,inputMap:collection.mutable.Map[String,String])={
    deleteSeasonInfoInStandingsTable(season,inputMap)
    df.withColumn("season",lit(season)).select(col("driver_id")
      ,col("positionVsRacePercentage").as("race_position_percentage")
      ,col("season")
      ,col("driverStandings").as("standings")
      ,col("totalPoints").as("total_points"))
      .write.mode("append").format("jdbc") .option("driver",inputMap("JDBCDriver"))
      .option("user",inputMap("JDBCUser"))
      .option("password",inputMap("JDBCPassword"))
      .option("url",inputMap("JDBCUrl"))
      .option("dbtable",s"${inputMap("schemaName")}.${inputMap("driverStandingsTable")}").save
  }
}
