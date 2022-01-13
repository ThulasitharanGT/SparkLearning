package org.controller

import org.util.SparkOpener

object tmpReduceGroup extends SparkOpener{

  val spark= SparkSessionLoc()
  import spark.implicits._

  case class tmpClass(col1:String,col2:String)
  def main(args:Array[String]):Unit ={

    val df=spark.read.format("csv").option("header","").option("inferSchema","").option("delimiter","").load.as[tmpClass]
    val tmp=df.reduce((x,y)=> x.col1 == y.col2 match {case true => x case false => y})

    val collectionMap=collection.mutable.Map[String,Int]("Apple"->2,"apple"->3,"banana"->1,"Banana"->6,"citrus"->8,"Citrus"->4)

    collectionMap.sameElements(collection.mutable.Map[String,Int]("s"->9))
    collectionMap.scanLeft(0)((x,y)=> y._2 >1 match {case true => x case false => y._2})
  }

}
