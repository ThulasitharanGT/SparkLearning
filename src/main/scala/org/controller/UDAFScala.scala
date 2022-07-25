package org.controller

import org.apache.spark.sql.{Encoder, Encoders, Row}
import org.apache.spark.sql.expressions.{Aggregator, MutableAggregationBuffer, UserDefinedAggregateFunction, Window}
import org.apache.spark.sql.functions.{asc, col, desc, explode}
import org.apache.spark.sql.types.{DataType, StructType}
// spark-submit --class org.controller.UDAFScala --driver-memory 512m --driver-cores 2 /home/raptor/IdeaProjects/SparkLearning/build/libs/SparkLearning-1.0-SNAPSHOT.jar

object UDAFScala {

  def main(args:Array[String]):Unit ={

    val spark=org.apache.spark.sql.SparkSession.builder.master("local[*]").getOrCreate
  spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._

   val trackRefDF= Seq((1,1,2.45)
     ,(1,2,3.85)
     ,(1,3,1.98)
     ,(2,1,3.45)
     ,(2,2,3.72)
     ,(2,3,2.98)
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

   /* joinedDF.show(false)
    joinedDF.as[lapIn].show(false)
*/
    // val avgSpeed = findSpeedPerSector.toColumn.name("speed")

   Seq(1,2,3).map(x => joinedDF.filter(s"sectorId= ${x}").as[lapIn].
     select( findSpeedPerSector.toColumn.name("speed"))).reduce(_.union(_)).show(false)

    Seq(1,2,3).map(x => joinedDF.filter(s"lapNo= ${x}").as[lapIn]
      .select(averageSpeedPerLap.toColumn.name("speed"))).reduce(_.union(_)).show(false)

    joinedDF.as[lapIn].select(averageSpeedPerLapAdvanced.toColumn.name("speed")).select(explode(col("calculatedData")).as("exploded")).select(col("exploded.*")).orderBy("trackId|driverId|lapId".split("\\|").map(asc) :+ desc("sectorId") :_*).show(false)


    val dataOfLapDriver2=Seq((1,2,1,1,34.5)
      ,(1,2,1,2,25.9)
      ,(1,2,1,3,41.3)
      ,(1,2,2,1,21.6)
      ,(1,2,2,2,31.5)
      ,(1,2,2,3,34.8)
      ,(1,2,3,1,28.5)
      ,(1,2,3,2,18.9)
      ,(1,2,3,3,31.4)
      ,(2,1,1,1,41.5)
      ,(2,1,1,2,38.9)
      ,(2,1,1,3,35.2)
    ).toDF("trackId,driverId,lapNo,sectorId,timeTaken".split(","):_*).union(dataOfLap)


    val joinedDF2= dataOfLapDriver2.join(trackRefDF,Seq("trackId","sectorId"))

    joinedDF2.as[lapIn].select(averageSpeedPerLapAdvancedPerDriver.toColumn.name("speed")).select(explode(col("calculatedData")).as("exploded")).select(col("exploded.*")).orderBy("trackId|driverId|lapId".split("\\|").map(asc) :+ desc("sectorId") :_*).show(200,false)

    joinedDF2.as[lapIn].withColumn("avgSpeedOfLap",new udafOriginal().apply(col("distanceInKM"),col("timeTaken")).over(Window.partitionBy("trackId,driverId,lapNo".split(",").map(col):_*))).show(false)
  }

  def getHour(timeInSeconds:Double) = (timeInSeconds / 60.0 ) / 60.0

}

// take the sector distance from another table
/*

Seq((1,1,2.45)).toSeq("trackId,sectorId,distanceInKM".split(","):_*)

*/

import org.apache.spark.sql.types._

class udafOriginal extends UserDefinedAggregateFunction
{
  override def inputSchema: StructType = new StructType(Array(StructField("distanceInKm",DoubleType,false),
    StructField("timeTaken",DoubleType,false)))

  override def bufferSchema: StructType = new StructType(Array(StructField("distanceInKm",DoubleType,false),
    StructField("timeTaken",DoubleType,false)))

  override def dataType: DataType = DoubleType

  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit = Array((0,0.0),(1,0.0)).map(x => buffer.update(x._1,x._2) )

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit =
    Array((0,input.getAs[Double]("distanceInKM")),(1,input.getAs[Double]("timeTaken"))).map(x => buffer.update(x._1,x._2 + buffer.getAs[Double](x._1)))

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit =
    {
      buffer1.update(0,buffer2.getAs[Double]("distanceInKM") + buffer1.getAs[Double](0))
      buffer1.update(1,buffer2.getAs[Double]("timeTaken") + buffer1.getAs[Double](1))
    }
    // Array((0,buffer2.getAs[Double]("distanceInKM")),(1,UDAFScala.getHour(buffer2.getAs[Double]("timeTaken"))),(2,1)).map(x => buffer1.update(x._1+buffer1.get(x._1),x._2 + buffer1.get(x._1)))

  override def evaluate(buffer: Row): Any = buffer.getAs[Double]("distanceInKm") /  UDAFScala.getHour(buffer.getAs[Double]("timeTaken"))
}

class udafTotalOfLapTimeSpeedAndDistance extends UserDefinedAggregateFunction
{
  override def inputSchema: StructType = new StructType(Array(StructField("distanceInKm",DoubleType,false),
    StructField("timeTaken",DoubleType,false)))

  override def bufferSchema: StructType = new StructType(Array(StructField("distanceInKm",DoubleType,false),
    StructField("timeTaken",DoubleType,false)))

  override def dataType: DataType = DoubleType

  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit = Array((0,0.0),(1,0.0)).map(x => buffer.update(x._1,x._2) )

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit =
    Array((0,input.getAs[Double]("distanceInKM")),(1,input.getAs[Double]("timeTaken"))).map(x => buffer.update(x._1,x._2 + buffer.getAs[Double](x._1)))

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit =
  {
    buffer1.update(0,buffer2.getAs[Double]("distanceInKM") + buffer1.getAs[Double](0))
    buffer1.update(1,buffer2.getAs[Double]("timeTaken") + buffer1.getAs[Double](1))
  }
  // Array((0,buffer2.getAs[Double]("distanceInKM")),(1,UDAFScala.getHour(buffer2.getAs[Double]("timeTaken"))),(2,1)).map(x => buffer1.update(x._1+buffer1.get(x._1),x._2 + buffer1.get(x._1)))

  override def evaluate(buffer: Row): Any = buffer.getAs[Double]("distanceInKm") /  UDAFScala.getHour(buffer.getAs[Double]("timeTaken"))
}


class lapData extends Product {
val trackId: Int = 0
val  driverId: Int = 0
val timeTaken: Double = 0.0
val distanceInKM: Double = 0.0

  override def productElement(n: Int): Any = {}
  override def productArity: Int = 0
  override def canEqual(that: Any): Boolean = that.equals(this)
}

case class groupingInfo(trackId:Int,lapId:Int)

case class lapIn(override val trackId:Int,override val driverId:Int,lapNo:Int,sectorId:Int,override val timeTaken:Double,override val distanceInKM:Double) extends lapData {
  override def toString =s"(trackId=${this.trackId},driverId=${this.driverId},lapNo=${this.lapNo},sectorId=${this.sectorId},timeTaken=${this.timeTaken},distanceInKM=${this.distanceInKM})"
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

case class lapOutTotal(calculatedData:Array[lapInterPerLapTotal]) extends lapData

case class lapInterPerLapTotal(override val trackId:Int, override val driverId:Int,lapId:Int,sectorId:Int, override val timeTaken:Double, avgSpeed:Double, override val distanceInKM:Double) extends lapData{
  override def toString =s"(trackId=${this.trackId},driverId=${this.driverId},sectorID=${this.sectorId},timeTaken=${this.timeTaken},avgSpeed=${this.avgSpeed},distanceInKM=${this.distanceInKM})"
}

case class fastestLap(lapId:Int,avgSpeed:Double)  extends lapData

object findSpeedPerSector extends UDAFAggregator[lapIn,lapInterPerSector,lapOutPerSector] {
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

 object averageSpeedPerLap extends UDAFAggregator[lapIn,lapIOPerLap,lapIOPerLap]{


   override def zero: lapIOPerLap = lapIOPerLap(0,0,0.0,0.0,0.0)

   override def reduce(b: lapIOPerLap, a: lapIn): lapIOPerLap =
     lapIOPerLap(a.trackId,a.driverId,a.timeTaken+b.timeTaken,0.0,a.distanceInKM)


   override def merge(b1: lapIOPerLap, b2: lapIOPerLap): lapIOPerLap =
     b2.copy(distanceInKM = b1.distanceInKM+b2.distanceInKM,timeTaken = b1.timeTaken+b2.timeTaken)


   override def finish(reduction: lapIOPerLap): lapIOPerLap =
     reduction.copy(avgSpeed = (reduction.distanceInKM / ((reduction.timeTaken / 60.0 )/60.0)))

 }

import scala.util.{Try,Success,Failure}

object averageSpeedPerLapAdvanced extends UDAFAggregator[lapIn,lapInterPerLapTotal,lapOutTotal]{

  val outputBufferMap=collection.mutable.Map[groupingInfo,lapOutTotal]()

  // [driverId,sectorId,[lapId's]]
  val sectorLapMap=collection.mutable.Map[Int,Set[Int]]()

  def appendToOutputBufferMap(incomingData:lapInterPerLapTotal)= Try{outputBufferMap(groupingInfo(incomingData.trackId,incomingData.lapId))} match {
    case Success(s) => outputBufferMap.put(groupingInfo(incomingData.trackId,incomingData.lapId),lapOutTotal(s.calculatedData :+ incomingData))
      incomingData
    case Failure(f) =>outputBufferMap.put(groupingInfo(incomingData.trackId,incomingData.lapId),lapOutTotal(Array(incomingData)))
      incomingData
  }

  def addSectorAndLapInfo(sectorId:Int,lapId:Int)=sectorLapMap.get(sectorId) match {
    case Some(laps) => sectorLapMap.put(sectorId,laps ++ Set(lapId))
    case None =>  sectorLapMap.put(sectorId,Set(lapId))
  }

  override def zero: lapInterPerLapTotal = lapInterPerLapTotal(0,0,0,0,0.0,0.0,0.0)

  override def reduce(b: lapInterPerLapTotal, a: lapIn)  = {
    addSectorAndLapInfo(a.sectorId,a.lapNo)
    appendToOutputBufferMap(lapInterPerLapTotal(a.trackId,a.driverId,a.lapNo,a.sectorId,a.timeTaken,0.0,a.distanceInKM))
  }

  override def merge(b1: lapInterPerLapTotal, b2: lapInterPerLapTotal) = b1 // no logic here. everything is collected in map and used in finish

  override def finish(reduction: lapInterPerLapTotal): lapOutTotal = {


    outputBufferMap.map(x =>
      outputBufferMap.put(x._1,
        lapOutTotal(x._2.calculatedData.map(x => x.copy(avgSpeed = x.distanceInKM / UDAFScala.getHour(x.timeTaken))) :+
          x._2.calculatedData.head.copy(sectorId = 0,
            timeTaken = x._2.calculatedData.map(_.timeTaken).reduce(_ + _),
            distanceInKM = x._2.calculatedData.map(_.distanceInKM).reduce(_ + _),
            avgSpeed = x._2.calculatedData.map(_.distanceInKM).reduce(_ + _) / UDAFScala.getHour(x._2.calculatedData.map(_.timeTaken).reduce(_ + _)))
        )))

    lapOutTotal( outputBufferMap.foldLeft(Array.empty[lapInterPerLapTotal])((collector,input) => collector ++ input._2.calculatedData) match {
      case value =>
        println(s"value ${value.deep}")
        println(s"sectorLapMap ${sectorLapMap}")
        outputBufferMap.map(x => s"outputBufferMap ${x._1} ${x._2.toString}")

        // avg sector time , lap id -1
          sectorLapMap.keys.foldLeft(value)((totalSet, sectorId) => totalSet :+ totalSet.filter(_.sectorId==sectorId).head.copy(lapId = -1,
          avgSpeed = totalSet.filter(_.sectorId == sectorId).map(_.avgSpeed).reduce(_ + _) / sectorLapMap(sectorId).size,
          timeTaken = totalSet.filter(_.sectorId == sectorId).map(_.timeTaken).reduce(_ + _) / sectorLapMap(sectorId).size)) :+
          value.filter(_.sectorId == 0).sortWith(_.timeTaken < _.timeTaken).head.copy(sectorId = -1) // fastest lap
    } )

  }
}


object averageSpeedPerLapAdvancedPerDriver extends UDAFAggregator[lapIn,lapInterPerLapTotal,lapOutTotal]{
 /* type driver=Int
  type track=Int
  type lap=Int
  type keyInfo = (driver,track)

  val driverIdTrackIdMap=collection.mutable.Map[keyInfo,collection.mutable.Map[lap,Array[lapInterPerLapTotal]]]()

*/
// driverId,trackId  => lap
  val driverIdTrackIdMap=collection.mutable.Map[(Int,Int),collection.mutable.Map[Int,Array[lapInterPerLapTotal]]]()

  override def zero: lapInterPerLapTotal = lapInterPerLapTotal(0,0,0,0,0.0,0.0,0.0)

  val addInfoToMap= (incomingRow:lapInterPerLapTotal) =>
    driverIdTrackIdMap.get((incomingRow.driverId,incomingRow.trackId)) match {
      case Some(map) =>
        Try{map(incomingRow.lapId)} match {
          case Success(s) =>
            driverIdTrackIdMap.put((incomingRow.driverId,incomingRow.trackId),
              driverIdTrackIdMap((incomingRow.driverId,incomingRow.trackId)) match {
                case value =>
                  value.put(incomingRow.lapId,s :+incomingRow )
                  value
              })
          case Failure(f) =>
            driverIdTrackIdMap.put((incomingRow.driverId,incomingRow.trackId),
              driverIdTrackIdMap((incomingRow.driverId,incomingRow.trackId)) match {
                case value =>
                  value.put(incomingRow.lapId,Array(incomingRow))
                  value
              })
        }

      case None =>
        driverIdTrackIdMap.put((incomingRow.driverId,incomingRow.trackId),
          collection.mutable.Map[Int,Array[lapInterPerLapTotal]](incomingRow.lapId -> Array(incomingRow)))
    }



  // calculate per sector, while adding it to map
  override def reduce(b: lapInterPerLapTotal, a: lapIn): lapInterPerLapTotal = {
    println(s"incoming record ${a.toString}")
    addInfoToMap(lapInterPerLapTotal(a.trackId,a.driverId,a.lapNo,a.sectorId,a.timeTaken,a.distanceInKM / UDAFScala.getHour(a.timeTaken),a.distanceInKM))
    b
  }

  override def merge(b1: lapInterPerLapTotal, b2: lapInterPerLapTotal): lapInterPerLapTotal = b1 // nothing here

  override def finish(reduction: lapInterPerLapTotal): lapOutTotal = {
    driverIdTrackIdMap.foreach(x =>{ println(s"${x._1}")
      x._2.map(z => {println(s" ${z._1}")
        z._2.deep.map(k => println(s" ${k.toString}"))
      })
    })

    lapOutTotal( driverIdTrackIdMap.flatMap(_._2.flatMap(
      x => x._2 :+ x._2.head.copy(sectorId = 0 , avgSpeed = x._2.map(_.avgSpeed).reduce(_+_) / x._2.size
        , distanceInKM = x._2.map(_.distanceInKM).reduce(_+_)
        , timeTaken = x._2.map(_.timeTaken).reduce(_+_)
      )
    )).groupBy(x=> (x.trackId,x.driverId)).flatMap(values => {
      values._2.toList match {
        case listValues=> listValues:+ listValues.filter(_.sectorId ==0).sortWith(_.avgSpeed < _.avgSpeed).last.copy(sectorId = -1)
      }
    }).toArray )
  }



}