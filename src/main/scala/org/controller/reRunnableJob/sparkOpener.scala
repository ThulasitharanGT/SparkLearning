package org.controller.reRunnableJob

import org.apache.spark.sql.SparkSession

trait sparkOpener {
  def sparkSessionOpen(name:String ="tempSession") = SparkSession.builder().appName(name).config("spark.sql.warehouse.dir", "/user/raptor/tmp/hive/warehouse2/").enableHiveSupport().getOrCreate()

}
