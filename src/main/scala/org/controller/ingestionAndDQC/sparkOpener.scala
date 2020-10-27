package org.controller.ingestionAndDQC

import org.apache.spark.sql.SparkSession
trait sparkOpener {
def sparkSessionOpen(name:String ="tempSession") = SparkSession.builder().appName(name).getOrCreate()
}
