package org.controller

import org.apache.spark.sql.expressions.Window
import org.util.{SparkOpener,readWriteUtil}
import org.constants.projectConstants
import org.apache.spark.sql.functions._

object windowFunctionEg extends  SparkOpener{
val spark=SparkSessionLoc("spark for window functions")
  def main(args: Array[String]): Unit = {
    val basePath=System.getProperty("user.dir")+projectConstants.pathSep+"Input"+projectConstants.pathSep

    val inputMap=collection.mutable.Map[String,String]()
    inputMap.put(projectConstants.fileTypeArgConstant,projectConstants.fileTypeCsvValue)
    inputMap.put(projectConstants.basePathArgConstant,basePath)
    inputMap.put(projectConstants.fileFormatArg,projectConstants.csvFormat)
    inputMap.put(projectConstants.filePathArgValue,basePath+"dept.txt")
    inputMap.put(projectConstants.inferSchemaArgConstant,projectConstants.stringTrue)
    inputMap.put(projectConstants.delimiterArgConstant,projectConstants.delimiterOr)
    inputMap.put(projectConstants.headerArgConstant,projectConstants.stringTrue)
    val dept_df = readWriteUtil.readDF(spark,inputMap)
    dept_df.createOrReplaceTempView("dept_view")

    val dept_df_mod=dept_df.groupBy("dept_id","dept_name","Month_Year").agg(sum("Reveue").as("sum_Reveue"))

    val dept_df_window =Window.orderBy(desc("sum_Reveue"))   //Window.partitionBy("dept_name").orderBy(desc("sum_Reveue"))  -- internally partitions inside the column

    val dept_df_window_partition =Window.partitionBy("dept_name").orderBy(desc("sum_Reveue"))

    val denseRankFun = dense_rank().over(dept_df_window )

    val rankFun = rank().over(dept_df_window )

    val percentRankFun = percent_rank().over(dept_df_window )

    dept_df_mod.withColumn("rank", denseRankFun).show

    dept_df_mod.withColumn("rank", rankFun).show

    dept_df_mod.withColumn("rank", percentRankFun).show

    // partitoned

    val denseRankFunPar = dense_rank().over(dept_df_window_partition )

    val rankFunPar = rank().over(dept_df_window_partition )

    val percentRankFunPar = percent_rank().over(dept_df_window_partition )

    dept_df_mod.withColumn("rank", denseRankFunPar).show

    dept_df_mod.withColumn("rank", rankFunPar).show

    dept_df_mod.withColumn("rank", percentRankFunPar).show




    val dept_df_window_par1 = Window.partitionBy("dept_name").orderBy(desc("Reveue"))

    val rankFunPar1 = rank().over(dept_df_window_par1)

    dept_df.withColumn("rank", rankFunPar1)


    dept_df_mod.withColumn("rank", rankFunPar)


    val dept_df_window_order = Window.orderBy(desc("sum_Reveue"))

    val rankFunOrd = rank().over(dept_df_window_order)

    dept_df_mod.withColumn("rank", rankFunOrd)

  }


}
