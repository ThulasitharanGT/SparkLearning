cd /home/raptor/IdeaProjects/SparkLearning/build/libs/


packages=org.apache.kafka:kafka-clients:2.3.1,org.apache.kafka:kafka_2.11:2.3.1,org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.0,io.delta:delta-core_2.11:0.5.0
jarName=SparkLearning-1.0-SNAPSHOT.jar
bootstrapServers=localhost:9092,localhost:9093,localhost:9094 
keyDeserializer=org.apache.kafka.common.serialization.StringSerializer 
valueDeserializer=org.apache.kafka.common.serialization.StringSerializer 
topicName=$1
outputPath=hdfs://localhost/user/raptor/kafka/stream/output/carSensorDataFromStream/
checkPointLocation=hdfs://localhost/user/raptor/kafka/stream/checkpoint/checkPointForStreamPullAndDeltaPushJob

#spark-submit --class org.controller.kafkaStreamExample.kafkaStreamRead  --num-executors 1 --executor-memory 1g --executor-cores 2 --driver-cores 1 --driver-memory 1g --packages org.apache.kafka:kafka-clients:2.3.1,org.apache.kafka:kafka_2.11:2.3.1,org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.0,io.delta:delta-core_2.11:0.5.0  SparkLearning-1.0-SNAPSHOT.jar bootStrapServer=localhost:9092,localhost:9093,localhost:9094 keyDeserializer=org.apache.kafka.common.serialization.StringDeserializer valueDeserializer=org.apache.kafka.common.serialization.StringDeserializer topicName=CarSensor checkPointLocation=hdfs://localhost/user/raptor/kafka/stream/checkpoint/checkPointForStreamPullAndDeltaPushJob outputPath=hdfs://localhost/user/raptor/kafka/stream/output/carSensorDataFromStream/



spark-submit --class org.controller.kafkaStreamExample.kafkaStreamRead  --num-executors 1 --executor-memory 1g --executor-cores 2 --driver-cores 1 --driver-memory 1g --packages $packages  $jarName bootStrapServer=$bootstrapServers keyDeserializer=$keyDeserializer valueDeserializer=$valueDeserializer topicName=$topicName checkPointLocation=$checkPointLocation outputPath=$outputPath
