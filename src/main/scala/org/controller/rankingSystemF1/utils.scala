package org.controller.rankingSystemF1
import org.apache.spark.sql.types._

object utils {

  val schemaOfOuterLayer=new StructType(Array(StructField("messageType",StringType,true)
    ,StructField("messageTimestamp",StringType,true)
    ,StructField("incomingMessage",StringType,true)))

  val schemaOfDriver=new StructType(Array(StructField("driverId",StringType,true)
    ,StructField("driverName",StringType,true),StructField("season",IntegerType,true)))

  val schemaOfRace=new StructType(Array(StructField("raceId",StringType,true)
    ,StructField("raceTrackId",StringType,true),StructField("raceDate",StringType,true)
    ,StructField("season",IntegerType,true)))

  val schemaOfRaceTrack=new StructType(Array(StructField("raceTrackId",StringType,true),StructField("raceVenue",StringType,true)))


  val schemaOfDriverRace=new StructType(Array(StructField("driverId",StringType,true)
    ,StructField("raceId",StringType,true),StructField("season",IntegerType,true)))

  val schemaOfDriverPoints=new StructType(Array(StructField("driverId",StringType,true)
    ,StructField("raceId",StringType,true),StructField("position",IntegerType,true),StructField("season",IntegerType,true)
    ,StructField("point",IntegerType,true)))
/*

  val schemaOfDriverTier2=new StructType(Array(StructField("driverId",StringType,true)
    ,StructField("driverName",StringType,true)
    ,StructField("season",StringType,true)
    ,StructField("incomingTs",StringType,true)
    ,StructField("driverRecordsPerSeason",ArrayType[StringType],true)))
*/


  case class driverRaceInfo(driverId:String,raceId:String,season:Int,incomingTs:java.sql.Timestamp){
    override def toString =s"""{"driverId":"${this.driverId}","raceId":"${this.raceId}","season":${this.season},"incomingTs":"${this.incomingTs}"}"""
    def toKafkaPayloadFormat= this.toString.replace("\"","\\\"")
  }

  case class driverRaceAndSeason(driverId:String,season:Int)
  case class driverTmpTable(resolveKey:String, messageJson:String, incomingTs:java.sql.Timestamp)
  case class driverTmpTableWithResult(resolveKey:String, messageJson:String, incomingTs:java.sql.Timestamp, resultType:String, rowsAffected:Int)

  case class driver_race_info(driver_id:String,race_id:String,messageTimestamp:java.sql.Timestamp)

  case class driver_race_infoWithResult(driver_id:String,race_id:String,messageTimestamp:java.sql.Timestamp,resultType:String,rowsAffected:Int)

  case class driverInfoWithReleaseInd(driverId:String,raceId:String,driverName:String,season:Int,incomingTs:java.sql.Timestamp,releaseInd:String)

  case class driverRaceInfoWithReleaseInd(driverId:String,raceId:String,season:Int,messageTimestamp:java.sql.Timestamp,releaseInd:String)

  case class driverPointInfo(driver_id:String,race_id:String,position:Int,season:String,point:Int,messageTimestamp:java.sql.Timestamp)

  case class driverPointInfoWithResult(driverId:String,raceId:String,position:Int,season:String,point:Int,messageTimestamp:java.sql.Timestamp,resultType:String,rowsAffected:Int)

  case class driverInfo(driverId:String,driverName:String,season:String,messageTimestamp:java.sql.Timestamp){
    override def toString=s"""{"driverId":"${this.driverId}","driverName":"${this.driverName}","season":${this.season},"messageTimestamp":"${this.messageTimestamp}"}"""
    def toKafkaPayload= this.toString.replace("\"","\\\"")
  }
  case class driverInfoWithResult(driverId:String,driverName:String,season:String,messageTimestamp:java.sql.Timestamp,resultType:String,rowsAffected:Int)

  case class driverPointsInfo(driverId:String,raceId:String,position:Int,point:Int,season:Int,messageTimestamp:java.sql.Timestamp){
    override def toString =s"""{"driverId":"${this.driverId}","raceId":${this.raceId},"position":${this.position},"point":${this.point},"season":${this.season},"messageTimestamp":"${this.messageTimestamp}"}"""
    def toKafkaPayloadFormat= this.toString.replace("\"","\\\"")
  }

  case class seasonReCalculationInfo(season:Int,incomingTs:java.sql.Timestamp){
    override def toString =s"""{"season":${this.season},"incomingTs":"${this.incomingTs}"}"""
    def toKafkaPayloadFormat= this.toString.replace("\"","\\\"")
  }

  case class raceInfo(raceId:String,raceTrackId:String,raceDate:String,season:Int,messageTimestamp:java.sql.Timestamp) {
    override def toString=s"""{"raceId":"${this.raceId}","raceTrackId":"${this.raceTrackId}","raceDate":"${this.raceDate}","season":${this.season},"messageTimestamp":"${this.messageTimestamp}"}"""
    def toKafkaPayload=this.toString.replace("\"","\\\"")
  }

  case class raceInfoWithResult(raceId:String,raceTrackId:String,raceDate:String,season:Int,messageTimestamp:java.sql.Timestamp,resultType:String,rowsAffected:Int)
  case class raceTrackInfo(raceTrackId:String,raceVenue:String,messageTimestamp:java.sql.Timestamp) {
    override def toString =s"""{"raceTrackId":"${this.raceTrackId}","raceVenue":"${this.raceVenue}","messageTimestamp":"${this.messageTimestamp}"}"""
    def toKafkaPayload=this.toString.replace("\"","\\\"")
  }
  case class raceTrackInfoWithResult(raceTrackId:String,raceVenue:String,messageTimestamp:java.sql.Timestamp,resultType:String,rowsAffected:Int)

  def getInputMap(args:Array[String])={
    val inputMap=collection.mutable.Map[String,String]()
    for(arg <- args)
    {
      val argSplit=arg.split("=",2)
      val keyPart=argSplit(0)
      val valPart=argSplit(1)
      inputMap.put(keyPart,valPart)
    }
    inputMap
  }
  /*
  case class driverTier2(driverId:String,driverName:String,season:Int,incomingTs:java.sql.Timestamp,driverRecordsPerSeason:Array[driverTier1]) {
    override def toString =s"""{"driverId":"${this.driverId}","driverName":"${this.driverName}","season":${this.season},"incomingTs":"${this.incomingTs}","driverRecordsPerSeason":["${this.driverRecordsPerSeason.map(_.toKafkaPayloadFormat).mkString("\",\"")}"]}"""
  }*/
  def getResultSet(conn:java.sql.Connection,query:String)= conn.prepareStatement(query).executeQuery


  def getJDBCConnection(inputMap:collection.mutable.Map[String,String])=java.sql.DriverManager.getConnection(inputMap("JDBCUrl"),getJDBCProps(inputMap))

  def getJDBCProps(inputMap:collection.mutable.Map[String,String])={
    Class.forName(inputMap("JDBCDriver"))
    val props=getProps
    props.put("url",inputMap("JDBCUrl"))
    props.put("user",inputMap("JDBCUser"))
    props.put("password",inputMap("JDBCPassword"))
    props
  }
  def getProps= new java.util.Properties

  def getKafkaProps(inputMap:collection.mutable.Map[String,String])={
    val props=getProps
    props.put("bootstrap.servers",inputMap("bootStrapServer"))
    props.put("key.serializer",inputMap("keySerializer"))
    props.put("value.serializer",inputMap("valueSerializer"))
    props
  }
  def getProducerObj(inputMap:collection.mutable.Map[String,String]) =new org.apache.kafka.clients.producer.KafkaProducer[String,String](getKafkaProps(inputMap))

  def sendMessageToKafka(msg:String,inputMap:collection.mutable.Map[String,String])={
    val producerObj=getProducerObj(inputMap)
    producerObj.send(getProducerRecord(msg,inputMap))
    producerObj.close
  }

  def getProducerRecord(msg:String,inputMap:collection.mutable.Map[String,String]) = new org.apache.kafka.clients.producer.ProducerRecord[String,String](inputMap("outputTopic"),msg)


}
