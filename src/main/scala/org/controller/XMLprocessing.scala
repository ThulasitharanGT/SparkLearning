package org.controller

import scala.xml.{XML,Node,Elem}
object XMLprocessing {
def main(args:Array[String]):Unit={
  val tmpXML="""<formula1>
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
"""

  val tmpXMLParsed=XML.loadString(tmpXML)
  val teamsTree=tmpXMLParsed \\ "formula1" \\ "Teams" \\ "team"
  //val driversAndTeams= teamsTree.map(x => ((x \\ "teamName").map(_.child.text).head ,(x \\ "drivers" \\ "driverName").flatMap(_.child)))


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





}
case class driverInfo(name:String,raceEntries:String,avgFinishPosition:String,points:String,nationality:String)

  def driverInfoGenerator(node: Node) ={
    //val driverBuffer=collection.mutable.ArrayBuffer[driverInfo]()
    val driverInfoNode= node \\ "driver"
    val driverName=  (driverInfoNode \\"driverName").map(_.child.text).head
    val driverNationality=  (driverInfoNode \\ "@nationality").map(_.text).head
    val driverRaceEntries=  (driverInfoNode \\ "@raceEntries").map(_.text).head
    val driverAvgFinishPosition=  (driverInfoNode \\ "avgFinishPosition").map(_.child.text).head
    val driverPoints=  (driverInfoNode \\ "points").map(_.child.text).head
    driverInfo(driverName,driverRaceEntries,driverAvgFinishPosition,driverPoints,driverNationality)
  }

}
