package org.controller

import org.apache.spark.sql.expressions.Window
import org.util.SparkOpener
import org.apache.spark.sql.functions._
object RankFunctionTest extends SparkOpener {
  val spark=SparkSessionLoc("deptRank")
  def main(args : Array[String])= {
    val dept_df = spark.read.option("header", "true").option("inferSchema", "true").format("csv").option("delimiter", "|").load(System.getProperty("user.dir")+"\\Input\\dept.txt")
    dept_df.createOrReplaceTempView("dept_view")
    spark.sql("select dept_id,dept_name, sum(Reveue), rank() over (order by sum(Reveue) desc) as Rank from dept_view  group by dept_id,dept_name").show(false)
    val dept_df_mod=dept_df.groupBy("dept_id","dept_name","Month_Year").agg(sum("Reveue").as("sum_Reveue"))

    val dept_df_window =Window.orderBy(desc("sum_Reveue"))   //Window.partitionBy("dept_name").orderBy(desc("sum_Reveue"))  -- internally partitions inside the column
    val dept_df_window_partition =Window.partitionBy("dept_name").orderBy(desc("sum_Reveue"))
    val denseRankFun = dense_rank().over(dept_df_window )
    val rankFun = rank().over(dept_df_window )
    val percentRankFun = percent_rank().over(dept_df_window)

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

    spark.close()
  }
}
