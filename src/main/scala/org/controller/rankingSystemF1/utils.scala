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

  val schemaOfPointsRecalculation=new StructType(Array(StructField("season",IntegerType,true)))

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

  case class racePointsInfo(position:Int, points:Int, season:Int, startDate:java.sql.Date, endDate:Option[java.sql.Date])

  case class racePointsInfoWithMeta(position:Int,points:Int,season:Int,startDate:java.sql.Date,endDate:Option[java.sql.Date],insertRecordCount:Int,updateRecordsCount:Int)

  case class driverRaceAndSeason(driverId:String,season:Int)
  case class driverTmpTable(resolveKey:String, messageJson:String, incomingTs:java.sql.Timestamp)
  case class driverTmpTableWithResult(resolveKey:String, messageJson:String, incomingTs:java.sql.Timestamp, resultType:String, rowsAffected:Int)

  case class driver_race_info(driver_id:String,race_id:String,messageTimestamp:java.sql.Timestamp)

  case class driver_race_infoWithResult(driver_id:String,race_id:String,messageTimestamp:java.sql.Timestamp,resultType:String,rowsAffected:Int)

  case class driverInfoWithReleaseInd(driverId:String,raceId:String,driverName:String,season:Int,incomingTs:java.sql.Timestamp,releaseInd:String)

  case class driverRaceInfoWithReleaseInd(driverId:String,raceId:String,season:Int,messageTimestamp:java.sql.Timestamp,releaseInd:String)

  case class driverPointInfo(driver_id:String,race_id:String,position:Int,season:String,point:Int,messageTimestamp:java.sql.Timestamp)

  case class driverPointInfoWithResult(driverId:String,raceId:String,position:Int,season:String,point:Int,messageTimestamp:java.sql.Timestamp,resultType:String,rowsAffected:Int)

  case class pointsReCalc(season:Int,messageTimeStamp:java.sql.Timestamp)
  case class driverInfo(driverId:String,driverName:String,season:String,messageTimestamp:java.sql.Timestamp){
    override def toString=s"""{"driverId":"${this.driverId}","driverName":"${this.driverName}","season":${this.season},"messageTimestamp":"${this.messageTimestamp}"}"""
    def toKafkaPayload= this.toString.replace("\"","\\\"")
  }
  case class driverInfoWithResult(driverId:String,driverName:String,season:String,messageTimestamp:java.sql.Timestamp,resultType:String,rowsAffected:Int)

  case class driverPointsInfo(driverId:String,raceId:String,position:Int,point:Int,season:Int,messageTimestamp:java.sql.Timestamp){
    override def toString =s"""{"driverId":"${this.driverId}","raceId":"${this.raceId}","position":${this.position},"point":${this.point},"season":${this.season},"messageTimestamp":"${this.messageTimestamp}"}"""
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

  /*
  *
DB schema:
*
create table kafka_db.driver_points(
driver_id varchar(10),
race_id varchar(10),
point integer,
position integer,
season integer,
incoming_timestamp timestamp);

create table kafka_db.driver_race_info(
driver_id varchar(10),
race_entry varchar(10),
incoming_timestamp timestamp);

create table kafka_db.driver_info(
driver_id varchar(10),
driver_name varchar(50),
season integer,
incoming_timestamp timestamp);

create table kafka_db.race_info(
race_id varchar(10),
race_track_id varchar(10),
race_date date,
race_season integer,
incoming_timestamp timestamp);

create table kafka_db.race_track_info(
race_track_id varchar(10),
race_venue varchar(50),
incoming_timestamp timestamp);

// scd 2 for this via batch job

create table kafka_db.points_position_map(
position varchar(10),
point varchar(50),
season integer,
start_date date,
end_date date default null,
incoming_timestamp timestamp);

create table kafka_db.driver_standings(
driver_id varchar(10),
race_position_percentage decimal(10,4),
season integer,
standings integer,
total_points decimal(10,4));

/// active , not active can be added

create table kafka_db.driver_point_temp(resolve_key varchar(20),
job_name varchar(50),incoming_timestamp timestamp,message_in_json varchar(500));

kafka topic
*
*
driver.race.points
driver.info
race.info
race.track.info
recalculate.points

*
*
*
// design diagram and doc later

-> driver_info (triggeres [driver_race_info and driver_race_point ])
-> race_track_info (triggeres [race_info ])

race_track_info  -> race_info (triggeres [driver_race_info and driver_race_point ])

race_info && driver_info -> [driver_race_info and driver_race_point]

[driver_race_info and driver_race_point]  -> triggeres [re calculation for the season]

[re calculation for the season] updates driver_standings table using points_position_map table

// points calculation season wise.

/ batch job for points position map



race track info

input - race.track.info
op - race.info

spark-submit --packages mysql:mysql-connector-java:8.0.26,org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2 --driver-memory 2g --driver-cores 2 --executor-memory 2g --executor-cores 2 --num-executors 2 --conf spark.driver.memoryOverhead=1g --class org.controller.rankingSystemF1.raceTrackToTable /home/raptor/IdeaProjects/SparkLearning/build/libs/SparkLearning-1.0-SNAPSHOT.jar outputTopic="race.info" bootStrapServer=localhost:8081,localhost:8082,localhost:8083 valueSerializer="org.apache.kafka.common.serialization.StringSerializer" keySerializer="org.apache.kafka.common.serialization.StringSerializer" checkPointLocation="hdfs://localhost:8020/user/raptor/kafka/raceTrackInfo" startingOffsets=latest topic="race.track.info" schemaName=kafka_db driverPointsTable=driver_points JDBCUrl="jdbc:mysql://localhost:3306/kafka_db?user=raptor?password=" JDBCPassword="" JDBCUser=raptor JDBCDriver="com.mysql.jdbc.Driver" stateExpiry=300000 raceInfoTable=race_info driverInfoTable=driver_info driverRaceInfoTable=driver_race_info driverPointsTemp=driver_point_temp raceTrackInfoTable=race_track_info

{"messageType":"raceTrackInfo","messageTimestamp":"2021-05-01 17:19:20","incomingMessage":"{\"raceTrackId\":\"RT001\",\"raceVenue\":\"Monaco\"}"}
{"messageType":"raceTrackInfo","messageTimestamp":"2021-05-01 17:19:20","incomingMessage":"{\"raceTrackId\":\"RT002\",\"raceVenue\":\"Canada\"}"}
{"messageType":"raceTrackInfo","messageTimestamp":"2021-05-01 17:19:20","incomingMessage":"{\"raceTrackId\":\"RT003\",\"raceVenue\":\"Azerbaijan\"}"}
{"messageType":"raceTrackInfo","messageTimestamp":"2021-05-01 17:19:20","incomingMessage":"{\"raceTrackId\":\"RT004\",\"raceVenue\":\"Australia\"}"}

driver info

input - driver.info
op - driver.race.points

spark-submit --packages mysql:mysql-connector-java:8.0.26,org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2 --driver-memory 2g --driver-cores 2 --executor-memory 2g --executor-cores 2 --num-executors 2 --conf spark.driver.memoryOverhead=1g --class org.controller.rankingSystemF1.driverToTable /home/raptor/IdeaProjects/SparkLearning/build/libs/SparkLearning-1.0-SNAPSHOT.jar outputTopic="driver.race.points" bootStrapServer=localhost:8081,localhost:8082,localhost:8083 valueSerializer="org.apache.kafka.common.serialization.StringSerializer" keySerializer="org.apache.kafka.common.serialization.StringSerializer" checkPointLocation="hdfs://localhost:8020/user/raptor/kafka/driverInfo" startingOffsets=latest topic="driver.info" schemaName=kafka_db driverPointsTable=driver_points JDBCUrl="jdbc:mysql://localhost:3306/kafka_db?user=raptor?password=" JDBCPassword="" JDBCUser=raptor JDBCDriver="com.mysql.jdbc.Driver" stateExpiry=300000 raceInfoTable=race_info driverInfoTable=driver_info driverRaceInfoTable=driver_race_info driverPointsTemp=driver_point_temp raceTrackInfoTable=race_track_info


{"messageType":"driverInfo","messageTimestamp":"2021-05-01 17:19:20","incomingMessage":"{\"driverId\":\"DR001\",\"driverName\":\"Senna\",\"season\":2020}"}

{"messageType":"driverInfo","messageTimestamp":"2021-05-01 17:19:20","incomingMessage":"{\"driverId\":\"DR002\",\"driverName\":\"Max\",\"season\":2020}"}

{"messageType":"driverInfo","messageTimestamp":"2021-05-01 17:19:20","incomingMessage":"{\"driverId\":\"DR003\",\"driverName\":\"Norris\",\"season\":2020}"}

{"messageType":"driverInfo","messageTimestamp":"2021-05-01 17:19:20","incomingMessage":"{\"driverId\":\"DR004\",\"driverName\":\"Norris\",\"season\":2020}"}
{"messageType":"driverInfo","messageTimestamp":"2021-05-01 17:19:20","incomingMessage":"{\"driverId\":\"DR005\",\"driverName\":\"Norris\",\"season\":2020}"}
{"messageType":"driverInfo","messageTimestamp":"2021-05-01 17:19:20","incomingMessage":"{\"driverId\":\"DR006\",\"driverName\":\"Norris\",\"season\":2020}"}
{"messageType":"driverInfo","messageTimestamp":"2021-05-01 17:19:20","incomingMessage":"{\"driverId\":\"DR007\",\"driverName\":\"Norris\",\"season\":2020}"}
{"messageType":"driverInfo","messageTimestamp":"2021-05-01 17:19:20","incomingMessage":"{\"driverId\":\"DR008\",\"driverName\":\"Norris\",\"season\":2020}"}


race info to table

input - race.info
op - driver.race.points

spark-submit --packages mysql:mysql-connector-java:8.0.26,org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2 --driver-memory 2g --driver-cores 2 --executor-memory 2g --executor-cores 2 --num-executors 2 --conf spark.driver.memoryOverhead=1g --class org.controller.rankingSystemF1.raceInfoToTable /home/raptor/IdeaProjects/SparkLearning/build/libs/SparkLearning-1.0-SNAPSHOT.jar outputTopic="driver.race.points" bootStrapServer=localhost:8081,localhost:8082,localhost:8083 valueSerializer="org.apache.kafka.common.serialization.StringSerializer" keySerializer="org.apache.kafka.common.serialization.StringSerializer" checkPointLocation="hdfs://localhost:8020/user/raptor/kafka/raceInfo" startingOffsets=latest topic="race.info" schemaName=kafka_db driverPointsTable=driver_points JDBCUrl="jdbc:mysql://localhost:3306/kafka_db?user=raptor?password=" JDBCPassword="" JDBCUser=raptor JDBCDriver="com.mysql.jdbc.Driver" stateExpiry=300000 raceInfoTable=race_info driverInfoTable=driver_info driverRaceInfoTable=driver_race_info driverPointsTemp=driver_point_temp raceTrackInfoTable=race_track_info

{"messageType":"raceInfo","messageTimestamp":"2021-11-01 17:19:20","incomingMessage":"{\"raceId\":\"RC001\",\"raceTrackId\":\"RT002\",\"raceDate\":\"2020-04-02\",\"season\":2020}"}
{"messageType":"raceInfo","messageTimestamp":"2021-11-01 17:19:20","incomingMessage":"{\"raceId\":\"RC002\",\"raceTrackId\":\"RT001\",\"raceDate\":\"2020-05-02\",\"season\":2020}"}
{"messageType":"raceInfo","messageTimestamp":"2021-11-01 17:19:20","incomingMessage":"{\"raceId\":\"RC003\",\"raceTrackId\":\"RT002\",\"raceDate\":\"2020-05-03\",\"season\":2020}"}
{"messageType":"raceInfo","messageTimestamp":"2021-11-01 17:19:20","incomingMessage":"{\"raceId\":\"RC004\",\"raceTrackId\":\"RT001\",\"raceDate\":\"2020-05-04\",\"season\":2020}"}

{"messageType":"raceInfo","messageTimestamp":"2021-11-01 17:19:20","incomingMessage":"{\"raceId\":\"RC005\",\"raceTrackId\":\"RT003\",\"raceDate\":\"2020-05-04\",\"season\":2020}"}
{"messageType":"raceInfo","messageTimestamp":"2021-11-01 17:19:20","incomingMessage":"{\"raceId\":\"RC006\",\"raceTrackId\":\"RT004\",\"raceDate\":\"2020-05-04\",\"season\":2020}"}

input - driver.race.points
op - recalculate.points

spark-submit --packages mysql:mysql-connector-java:8.0.26,org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2 --driver-memory 2g --driver-cores 2 --executor-memory 2g --executor-cores 2 --num-executors 2 --conf spark.driver.memoryOverhead=1g --class org.controller.rankingSystemF1.driverRaceAndPointsInfo /home/raptor/IdeaProjects/SparkLearning/build/libs/SparkLearning-1.0-SNAPSHOT.jar outputTopic="recalculate.points" bootStrapServer=localhost:8081,localhost:8082,localhost:8083 valueSerializer="org.apache.kafka.common.serialization.StringSerializer" keySerializer="org.apache.kafka.common.serialization.StringSerializer" checkPointLocation="hdfs://localhost:8020/user/raptor/kafka/raceAndPointInfo" startingOffsets=latest topic="driver.race.points" schemaName=kafka_db driverPointsTable=driver_points JDBCUrl="jdbc:mysql://localhost:3306/kafka_db?user=raptor?password=" JDBCPassword="" JDBCUser=raptor JDBCDriver="com.mysql.jdbc.Driver" stateExpiry=300000 raceInfoTable=race_info driverInfoTable=driver_info driverRaceInfoTable=driver_race_info driverPointsTemp=driver_point_temp raceTrackInfoTable=race_track_info

driverRaceInfo
driverPointsInfo

{"messageType":"driverRaceInfo","messageTimestamp":"2021-11-01 17:19:20","incomingMessage":"{\"driverId\":\"DR001\",\"season\":2020,\"raceId\":\"RC001\"}"}

{"messageType":"driverPointsInfo","messageTimestamp":"2021-11-01 17:19:20","incomingMessage":"{\"driverId\":\"DR001\",\"raceId\":\"RC001\",\"position\":4,\"season\":2020,\"point\":12}"}

// grand parent check , with driver info

{"messageType":"driverPointsInfo","messageTimestamp":"2021-11-01 17:19:20","incomingMessage":"{\"driverId\":\"DR002\",\"raceId\":\"RC001\",\"position\":5,\"season\":2020,\"point\":10}"}

{"messageType":"driverRaceInfo","messageTimestamp":"2021-11-01 17:19:20","incomingMessage":"{\"driverId\":\"DR002\",\"season\":2020,\"raceId\":\"RC001\"}"}


// grand parent check , with race info

{"messageType":"driverPointsInfo","messageTimestamp":"2021-11-01 17:19:20","incomingMessage":"{\"driverId\":\"DR002\",\"raceId\":\"RC003\",\"position\":5,\"season\":2020,\"point\":10}"}
{"messageType":"driverRaceInfo","messageTimestamp":"2021-11-01 17:19:20","incomingMessage":"{\"driverId\":\"DR002\",\"season\":2020,\"raceId\":\"RC003\"}"}



// grand parent check , with race info and driver info

{"messageType":"driverPointsInfo","messageTimestamp":"2021-11-01 17:19:20","incomingMessage":"{\"driverId\":\"DR003\",\"raceId\":\"RC004\",\"position\":5,\"season\":2020,\"point\":10}"}
{"messageType":"driverRaceInfo","messageTimestamp":"2021-11-01 17:19:20","incomingMessage":"{\"driverId\":\"DR003\",\"season\":2020,\"raceId\":\"RC004\"}"}

*
* */

}
