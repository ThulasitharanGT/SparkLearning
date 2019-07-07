package controller
import util.SparkOpener
import org.apache.spark.sql.functions._

object seqOverTry extends SparkOpener {
  def main(args: Array[String]): Unit = {
    val spark = SparkSessionLoc("SparkSession")
    val df=spark.read.format("csv").option("header","true").option("inferSchema","true").option("delimiter","|").load("D:\\study\\seq_over_try.txt")
  // val df_temp=

     // df.selectExpr("Item",explode(split($"CountryList',',')) as County","profit").show()

    // df.select(s"*",explode(split(df.col("CountryList"),','))).show()
  //  df.select($"*",explode(split($"CountryList",","))).show() - not working in intelliji
/*
* in shell     df.select($"*",explode(split($"CountryList",","))).show()
+------+-----------+------+---+
|  Item|CountryList|Profit|col|
+------+-----------+------+---+
| Phone|USA,AUS,GER|  8000|USA|
| Phone|USA,AUS,GER|  8000|AUS|
| Phone|USA,AUS,GER|  8000|GER|
| Phone|    USA,AUS| 12000|USA|
| Phone|    USA,AUS| 12000|AUS|
| Phone|    USA,CHN| 23000|USA|
| Phone|    USA,CHN| 23000|CHN|
| Phone|GBR,CHN,AUS|  1000|GBR|
| Phone|GBR,CHN,AUS|  1000|CHN|
| Phone|GBR,CHN,AUS|  1000|AUS|
|Laptop|USA,AUS,GER| 12000|USA|
|Laptop|USA,AUS,GER| 12000|AUS|
|Laptop|USA,AUS,GER| 12000|GER|
|Laptop|    USA,AUS| 23000|USA|
|Laptop|    USA,AUS| 23000|AUS|
|Laptop|    USA,CHN|  1000|USA|
|Laptop|    USA,CHN|  1000|CHN|
|Laptop|GBR,CHN,AUS|  8000|GBR|
|Laptop|GBR,CHN,AUS|  8000|CHN|
|Laptop|GBR,CHN,AUS|  8000|AUS|
+------+-----------+------+---+


scala>     df.select($"*",explode(split($"CountryList",","))).drop("CountryList").show()
+------+------+---+
|  Item|Profit|col|
+------+------+---+
| Phone|  8000|USA|
| Phone|  8000|AUS|
| Phone|  8000|GER|
| Phone| 12000|USA|
| Phone| 12000|AUS|
| Phone| 23000|USA|
| Phone| 23000|CHN|
| Phone|  1000|GBR|
| Phone|  1000|CHN|
| Phone|  1000|AUS|
|Laptop| 12000|USA|
|Laptop| 12000|AUS|
|Laptop| 12000|GER|
|Laptop| 23000|USA|
|Laptop| 23000|AUS|
|Laptop|  1000|USA|
|Laptop|  1000|CHN|
|Laptop|  8000|GBR|
|Laptop|  8000|CHN|
|Laptop|  8000|AUS|
+------+------+---+


scala>     df.select($"*",explode(split($"CountryList",","))).drop("CountryList").groupBy("Item").agg(sum("Profit")).show
+------+-----------+
|  Item|sum(Profit)|
+------+-----------+
| Phone|      97000|
|Laptop|     108000|
+------+-----------+


scala>     df.select($"*",explode(split($"CountryList",","))).drop("CountryList").groupBy("Item","col").agg(sum("Profit")).show
+------+---+-----------+
|  Item|col|sum(Profit)|
+------+---+-----------+
| Phone|GER|       8000|
| Phone|USA|      43000|
| Phone|CHN|      24000|
|Laptop|USA|      36000|
|Laptop|GER|      12000|
| Phone|GBR|       1000|
|Laptop|GBR|       8000|
|Laptop|AUS|      43000|
| Phone|AUS|      21000|
|Laptop|CHN|       9000|
+------+---+-----------+

*
* */

  }

}
