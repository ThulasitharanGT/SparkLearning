package org.controller

import org.util.SparkOpener

object RevenueClass extends SparkOpener{

  val spark=SparkSessionLoc("SparkSession")

  def main(args: Array[String]): Unit
  =
  {
    val Revenue_df=spark.read.format("csv").option("header","true").option("delimiter","|").option("inferSchema","true").load(System.getProperty("user.dir")+"\\Input\\productrevenue.txt")
    Revenue_df.show
    Revenue_df.createOrReplaceTempView("Rev_view")
    spark.sql("select  product,category,revenue, dense_rank() over(partition by category order by revenue desc) as Rank from Rev_view").createOrReplaceTempView("Revenue_rank")
    spark.sql("select * from Revenue_rank ").show()
    spark.sql("select * from Revenue_rank having Rank >=2").show()
    spark.sql("select  product,category,revenue, dense_rank() over(partition by category order by revenue desc) as Rank from Rev_view").show
    spark.sql("select  product,category,revenue, percent_rank() over(partition by category order by revenue desc) as Rank from Rev_view").show
   // spark.sql("select  product,category,revenue, dense_rank() over(partition by category order by revenue desc) as Rank from Rev_view").show

  }
 // spark.close()

}
