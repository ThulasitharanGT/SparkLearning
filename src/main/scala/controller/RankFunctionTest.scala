package controller
import util.SparkOpener

object RankFunctionTest extends SparkOpener {
  val spark=SparkSessionLoc("deptRank")
  def main(args : Array[String])= {
    val dept_df = spark.read.option("header", "true").option("inferSchema", "true").format("csv").option("delimiter", "|").load("D:\\study\\dept.txt")
    dept_df.createOrReplaceTempView("dept_view")
    spark.sql("select dept_id,dept_name, sum(Reveue), rank() over (order by sum(Reveue) desc) as Rank from dept_view  group by dept_id,dept_name").show(false)
    spark.close()
  }
}
