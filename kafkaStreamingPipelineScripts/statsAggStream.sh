cd /home/raptor/IdeaProjects/SparkLearning/build/libs/

packages=io.delta:delta-core_2.11:0.5.0
jarName=SparkLearning-1.0-SNAPSHOT.jar
streamInputPath=hdfs://localhost/user/raptor/kafka/stream/output/carSensorDataFromStream/
statsOutputPath=hdfs://localhost/user/raptor/kafka/stream/output/carSensorDataFromStreamStats/
checkPointLocation=hdfs://localhost/user/raptor/kafka/stream/checkpoint/checkPointForStreamStatsAgg

spark-submit --class org.controller.kafkaStreamExample.streamAggregationStats  --num-executors 1 --executor-memory 1g --executor-cores 2 --driver-cores 1 --driver-memory 1g --packages $packages  $jarName checkPointLocation=$checkPointLocation statsOutputPath=$statsOutputPath streamInputPath=$streamInputPath
