
cd /home/raptor/IdeaProjects/SparkLearning/build/libs/

packages=io.delta:delta-core_2.11:0.5.0
jarName=SparkLearning-1.0-SNAPSHOT.jar
inputPath=hdfs://localhost/user/raptor/kafka/stream/output/carSensorDataFromStream/
outputPath=hdfs://localhost/user/raptor/kafka/stream/temp/consoleCarSensorDataFromStream/
checkPointLocation=hdfs://localhost/user/raptor/kafka/stream/checkpoint/checkPointForReadFromDeltaJob/


#spark-submit --class org.controller.kafkaStreamExample.deltaStreamRead  --num-executors 1 --executor-memory 1g --executor-cores 2 --driver-cores 1 --driver-memory 1g --packages io.delta:delta-core_2.11:0.5.0  SparkLearning-1.0-SNAPSHOT.jar inputPath=hdfs://localhost/user/raptor/kafka/stream/output/carSensorDataFromStream/ checkPointLocation=hdfs://localhost/user/raptor/kafka/stream/checkpoint/checkPointForReadFromDeltaJob/ outputPath=hdfs://localhost/user/raptor/kafka/stream/temp/consoleCarSensorDataFromStream/
 


spark-submit --class org.controller.kafkaStreamExample.deltaStreamRead  --num-executors 1 --executor-memory 1g --executor-cores 2 --driver-cores 1 --driver-memory 1g --packages $packages  $jarName inputPath=$inputPath checkPointLocation=$checkPointLocation outputPath=$outputPath
 
