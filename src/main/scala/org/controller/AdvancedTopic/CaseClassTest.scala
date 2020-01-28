package org.controller.AdvancedTopic
import org.util.SparkOpener

object CaseClassTest extends SparkOpener{

  def main(args: Array[String]): Unit = {
    val spark=SparkSessionLoc("Loc")
    import spark.implicits._
    val carDF=spark.read.option("header","true").format("csv").option("inferSchema","true").option("delimiter","|").load("D:\\study\\Avail_car.txt")
    carDF.show
    val carDS=carDF.map(row => CaseClassCar(row.getString(0),row.getString(1),row.getString(2),row.getInt(3),row.getInt(4),row.getTimestamp(5)))
    carDS.show //-- runs in spatk shell. doesn't support in intelliji

  }

}
