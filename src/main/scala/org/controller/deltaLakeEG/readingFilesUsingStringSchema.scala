package org.controller.deltaLakeEG

import org.constants.projectConstants
import org.util.SparkOpener

object readingFilesUsingStringSchema extends SparkOpener {
  val spark=SparkSessionLoc("schema test")
  def main(args: Array[String]): Unit = {
    val dfCsvNewTry=spark.read.format(projectConstants.csvFormat).option("header","true").option("delimiter","|").schema("dept_id integer,dept_name string,Reveue integer,Month_Year string").load(System.getProperty("user.dir")+projectConstants.pathSep+"Input"+projectConstants.pathSep+"dept.txt")
      dfCsvNewTry.show()
  }

}
