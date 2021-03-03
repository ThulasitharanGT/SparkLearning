package org.controller

import org.util.SparkOpener

import scala.xml.{Elem, Node, XML}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.util.readWriteUtil.scalaFileReader

object XMLprocessing extends SparkOpener{
  case class driverInfo(name:String,raceEntries:String,avgFinishPosition:String,points:String,nationality:String)
  case class teamInfo(name:String,drivers:Seq[driverInfo])
  case class teamInfoFlattened(name:String,driverDetail:driverInfo,constructorPoints:String,constructorStanding:String)
  case class teamInfoDeepFlattened(teamName:String,driverName:String,driverRaceEntries:String,driverAvgFinishPosition:String,driverPoints:String,driverNationality:String,constructorPoints:String,constructorStanding:String)

  def main(args:Array[String]):Unit={
val inputMap=collection.mutable.Map[String,String]()
    for(arg <- args)
      inputMap.put(arg.split("=",2)(0),arg.split("=",2)(1))
 /* val tmpXML="""<formula1>
<Teams>
<team>
<teamName>Redbull racing</teamName>
<constructorPoints>400</constructorPoints>
<constructorStanding>2nd</constructorStanding>
<drivers>
<driver raceEntries="100" nationality="Dutch">
<driverName>Max Verstappen</driverName>
<avgFinishPosition>3</avgFinishPosition>
<points>300</points>
</driver>
<driver raceEntries="50" nationality="Thai">
<driverName>Alex Albon</driverName>
<avgFinishPosition>10</avgFinishPosition>
<points>90</points>
</driver>
</drivers>
</team>
<team>
<teamName>Mclaren Racing</teamName>
<constructorPoints>350</constructorPoints>
<constructorStanding>3 rd</constructorStanding>
<drivers>
<driver raceEntries="100" nationality="British">
<driverName>Lando Norris</driverName>
<avgFinishPosition>6</avgFinishPosition>
<points>80</points>
</driver>
<driver raceEntries="90" nationality="Spain">
<driverName>Carlos Sainz</driverName>
<avgFinishPosition>5</avgFinishPosition>
<points>85</points>
</driver>
</drivers>
</team>
</Teams>
</formula1>
"""*/

    val tmpXML= scalaFileReader(inputMap("XMLFile")).mkString
    print(s"tmpXML - ${tmpXML}")
  val spark=SparkSessionLoc()
  val tmpXMLParsed=XML.loadString(tmpXML)
  val teamsTree=tmpXMLParsed \\ "formula1" \\ "Teams" \\ "team"
  //val driversAndTeams= teamsTree.map(x => ((x \\ "teamName").map(_.child.text).head ,(x \\ "drivers" \\ "driverName").flatMap(_.child)))

/*

  val driversAndTeams= teamsTree.map(x => ((x \\ "teamName").map(_.child.text).head , {
    val driverInfoArrayBuffer=collection.mutable.ArrayBuffer[driverInfo]()
    val driverNodes= x \\ "drivers" \\ "driver"
    for (driverNode <- driverNodes)
    {
      val driverName=  (driverNode \\"driverName").map(_.child.text).head
      val driverNationality=  (driverNode \\ "@nationality").map(_.text).head
      val driverRaceEntries=  (driverNode \\ "@raceEntries").map(_.text).head
      val driverAvgFinishPosition=  (driverNode \\ "avgFinishPosition").map(_.child.text).head
      val driverPoints=  (driverNode \\ "points").map(_.child.text).head
      driverInfoArrayBuffer+=driverInfo(driverName,driverRaceEntries,driverAvgFinishPosition,driverPoints,driverNationality)
    }
    driverInfoArrayBuffer
  }
  ))
*/

 // val driversAndTeams= teamsTree.map(x => ((x \\ "teamName").map(_.child.text).head ,(x \\ "teamName"\\"drivers"\\"driver").map(driverInfoGenerator)))
    import spark.implicits._
    val driversAndTeams= teamsTree.map(x => ((x \\ "teamName").map(_.child.text).head ,(x \\ "drivers"\\ "driver").map(driverInfoGenerator),(x \\ "constructorPoints").map(_.child.text).head,(x \\ "constructorStanding").map(_.child.text).head))

  val driverAndTeamsFlattened=driversAndTeams.flatMap(teamAndDriverInfoFlatten).toDF.as[teamInfoFlattened]
    val driverAndTeamInfoTmp=driverAndTeamsFlattened.map(teamAndDriverInfoDeepFlatten)
    val teamTotalPoints=driverAndTeamInfoTmp.groupBy("teamName").agg(sum(col("driverPoints").cast(IntegerType)).as("TeamPoints"))
    val driverAndTeamInfo= driverAndTeamInfoTmp.join(teamTotalPoints,Seq("teamName")).withColumn("pitStopPoints",when(col("constructorPoints").cast(IntegerType)-col("TeamPoints").cast(IntegerType) >0,col("constructorPoints").cast(IntegerType)-col("TeamPoints").cast(IntegerType) ).otherwise(lit("0")))
    driverAndTeamInfo.show(false)
    // run in spark 300
    // spark-submit --class org.controller.XMLprocessing --name tmpProgram --driver-memory 512m --driver-cores 2 --executor-memory 512m --executor-cores 2 --num-executors 1 file:///home/raptor/IdeaProjects/SparkLearning/build/libs/SparkLearning-1.0-SNAPSHOT.jar XMLFile=/home/raptor/IdeaProjects/SparkLearning/Input/tempInputs/xmlParsing.xml
}

  def driverInfoGenerator(node: Node) ={
    //val driverBuffer=collection.mutable.ArrayBuffer[driverInfo]()
    val driverInfoNode=  node //node \\ "driver"
    val driverName=  (driverInfoNode \\"driverName").map(_.child.text).head
    val driverNationality=  (driverInfoNode \\ "@nationality").map(_.text).head
    val driverRaceEntries=  (driverInfoNode \\ "@raceEntries").map(_.text).head
    val driverAvgFinishPosition=  (driverInfoNode \\ "avgFinishPosition").map(_.child.text).head
    val driverPoints=  (driverInfoNode \\ "points").map(_.child.text).head
    driverInfo(driverName,driverRaceEntries,driverAvgFinishPosition,driverPoints,driverNationality)
  }
  def teamAndDriverInfoFlatten(teamAndDriverInfo:(String, scala.collection.immutable.Seq[driverInfo], String, String)):Seq[teamInfoFlattened] ={
    val teamAndDriverInfoArrayBuffer=collection.mutable.ArrayBuffer[teamInfoFlattened]()
    val teamName=teamAndDriverInfo._1
    val constructorPoints=teamAndDriverInfo._3
    val constructorStanding=teamAndDriverInfo._4
    for(driverInfoTmp <- teamAndDriverInfo._2)
      teamAndDriverInfoArrayBuffer+=teamInfoFlattened(teamName,driverInfoTmp,constructorPoints,constructorStanding)
    teamAndDriverInfoArrayBuffer.toSeq
  }

  def teamAndDriverInfoDeepFlatten(teamAndDriverInfo:teamInfoFlattened) = teamInfoDeepFlattened(teamAndDriverInfo.name,teamAndDriverInfo.driverDetail.name,teamAndDriverInfo.driverDetail.raceEntries,teamAndDriverInfo.driverDetail.avgFinishPosition,teamAndDriverInfo.driverDetail.points,teamAndDriverInfo.driverDetail.nationality,teamAndDriverInfo.constructorPoints,teamAndDriverInfo.constructorStanding)
}
