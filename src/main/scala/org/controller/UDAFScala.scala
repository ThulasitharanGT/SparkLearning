package org.controller

import org.apache.spark.sql.Encoder
import org.apache.spark.sql.expressions.Aggregator
// spark-submit --class org.controller.UDAFScala --driver-memory 512m --driver-cores 2 /home/raptor/IdeaProjects/SparkLearning/build/libs/SparkLearning-1.0-SNAPSHOT.jar

object UDAFScala {

  def main(args:Array[String]):Unit ={

    val spark=org.apache.spark.sql.SparkSession.builder.master("local[*]").getOrCreate
  spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._

   val trackRefDF= Seq((1,1,2.45)
     ,(1,2,3.85)
     ,(1,3,1.98)
   ).toDF("trackId,sectorId,distanceInKM".split(","):_*)

    val dataOfLap=Seq((1,1,1,1,29.8)
      , (1,1,1,2,42.2)
      ,(1,1,1,3,35.6)
      , (1,1,2,1,27.3)
      , (1,1,2,2,41.9)
      ,(1,1,2,3,32.8)
     , (1,1,3,1,30.8)
      , (1,1,3,2,41.9)
      ,(1,1,3,3,36.7)
    ).toDF("trackId,driverId,lapNo,sectorId,timeTaken".split(","):_*)


    val joinedDF=trackRefDF.join(dataOfLap,Seq("trackId","sectorId"))

    joinedDF.show(false)
    joinedDF.as[lapIn].show(false)

    // val avgSpeed = findSpeedPerSector.toColumn.name("speed")

   Seq(1,2,3).map(x => joinedDF.filter(s"sectorId= ${x}").as[lapIn].
     select( findSpeedPerSector.toColumn.name("speed"))).reduce(_.union(_)).show(false)

    Seq(1,2,3).map(x => joinedDF.filter(s"lapNo= ${x}").as[lapIn]
      .select(averageSpeedPerLap.toColumn.name("speed"))).reduce(_.union(_)).show(false)

  }

}

// take the sector distance from another table
/*

Seq((1,1,2.45)).toSeq("trackId,sectorId,distanceInKM".split(","):_*)

*/

class lapData extends Product {
val trackId: Int = 0
val  driverId: Int = 0
val timeTaken: Double = 0.0
val distanceInKM: Double = 0.0

  override def productElement(n: Int): Any = {}
  override def productArity: Int = 0
  override def canEqual(that: Any): Boolean = that.equals(this)
}

case class lapIn(override val trackId:Int,override val driverId:Int,lapNo:Int,sectorId:Int,override val timeTaken:Double,override val distanceInKM:Double) extends lapData {
  override def toString =s"(trackId=${this.trackId},driverId=${this.driverId},sectorId=${this.sectorId},timeTaken=${this.timeTaken},distanceInKM=${this.distanceInKM})"
}
case class lapInterPerSector(override val trackId:Int,override val driverId:Int,sectorId:Int,override val timeTaken:Double,avgSpeed:Double,override val distanceInKM:Double) extends lapData{
  override def toString =s"(trackId=${this.trackId},driverId=${this.driverId},sectorId=${this.sectorId},timeTaken=${this.timeTaken},avgSpeed=${this.avgSpeed},distanceInKM=${this.distanceInKM})"
}
case class lapOutPerSector(override val trackId:Int,override val driverId:Int,sectorId:Int,avgTimeTaken:Double,avgSpeed:Double) extends lapData{
  override def toString =s"(trackId=${this.trackId},driverId=${this.driverId},sectorId=${this.sectorId},timeTaken=${this.timeTaken},avgTimeTaken=${this.avgTimeTaken},avgSpeed=${this.avgSpeed},distanceInKM=${this.distanceInKM})"
}

case class lapIOPerLap(override val trackId:Int,override val driverId:Int,override val timeTaken:Double,avgSpeed:Double,override val distanceInKM:Double) extends lapData{
  override def toString =s"(trackId=${this.trackId},driverId=${this.driverId},timeTaken=${this.timeTaken},avgSpeed=${this.avgSpeed},distanceInKM=${this.distanceInKM})"
}



case class fastestLap(lapId:Int,avgSpeed:Double)  extends lapData

object findSpeedPerSector extends UDAFextention[lapIn,lapInterPerSector,lapOutPerSector] {
  var lapSet:Set[Int] = Set.empty[Int]
  /* def this(totalRecords:Int){
      this()
      this.totalRecords=totalRecords
    } */
  override def zero: lapInterPerSector = {
    println(s"Defined")
    lapInterPerSector( 0,0,0, 0.0, 0.0, 0.0)
  }

  override def reduce(b: lapInterPerSector, a: lapIn): lapInterPerSector = {
    println(s"reduce lapSet ${lapSet}")
    println(s"reduce lapInter ${b.toString}")
    println(s"reduce lapIn ${a.toString}")
    lapSet+=a.lapNo
    lapInterPerSector(a.trackId,a.driverId,a.driverId,a.timeTaken+b.timeTaken,0.0,a.distanceInKM+b.distanceInKM)
  }

  // b1  is always the initialized one
  override def merge(b1: lapInterPerSector, b2: lapInterPerSector): lapInterPerSector = {
    println(s"merge")
    println(s"merge b1 ${b1.toString}")
    println(s"merge b2 ${b2.toString}")
    lapInterPerSector(b2.trackId, b2.driverId, b2.sectorId, b1.timeTaken + b2.timeTaken, 0.0, b1.distanceInKM + b2.distanceInKM)
  }

  override def finish(reduction: lapInterPerSector): lapOutPerSector = {
    println(s"reduction ${reduction.toString}")
    println(s"lapSet ${lapSet}")
    lapOutPerSector(reduction.trackId, reduction.driverId, reduction.sectorId, reduction.timeTaken / lapSet.size, reduction.distanceInKM / ((reduction.timeTaken /60.0) /60.0) ) // to hour
}
  override def bufferEncoder: Encoder[lapInterPerSector] = org.apache.spark.sql.Encoders.product[lapInterPerSector]
    //org.apache.spark.sql.Encoders.bean[lapInter](lapInter.getClass.asInstanceOf[Class[lapInter]])

  override def outputEncoder: Encoder[lapOutPerSector] = org.apache.spark.sql.Encoders.product[lapOutPerSector]
    //org.apache.spark.sql.Encoders.bean[lapOut](lapOut.getClass.asInstanceOf[Class[lapOut]])
}

 object averageSpeedPerLap extends UDAFextention[lapIn,lapIOPerLap,lapIOPerLap]{

  // val tmpMap=collection.mutable.Map[Int,Int]()

   override def zero: lapIOPerLap = lapIOPerLap(0,0,0.0,0.0,0.0)

   override def reduce(b: lapIOPerLap, a: lapIn): lapIOPerLap = {
     //tmpMap.put(a.sectorId,a.trackId)
     lapIOPerLap(a.trackId,a.driverId,a.timeTaken+b.timeTaken,0.0,a.distanceInKM)
   }

   override def merge(b1: lapIOPerLap, b2: lapIOPerLap): lapIOPerLap =
     b2.copy(distanceInKM = b1.distanceInKM+b2.distanceInKM,timeTaken = b1.timeTaken+b2.timeTaken)


   override def finish(reduction: lapIOPerLap): lapIOPerLap =
     reduction.copy(avgSpeed = (reduction.distanceInKM / ((reduction.timeTaken / 60.0 )/60.0)))

 }