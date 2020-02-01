cd /home/raptor/IdeaProjects/SparkLearning/build/libs/

#spark-submit --class org.controller.kafkaStreamExample.systemCommandRunnerForShellScript --num-executors 1 --executor-cores 2 --executor-memory 1g --driver-memory 1g --driver-cores 1 SparkLearning-1.0-SNAPSHOT.jar scriptsNamesWithParam=startExtractLoadSteam.sh~CarSensor

#spark-submit --class org.controller.kafkaStreamExample.systemCommandRunnerForShellScript --num-executors 1 --executor-cores 2 --executor-memory 1g --driver-memory 1g --driver-cores 1 SparkLearning-1.0-SNAPSHOT.jar scriptsNamesWithParam=startReadStream.sh

jarName=SparkLearning-1.0-SNAPSHOT.jar
scriptsNamesWithParam=$1

spark-submit --class org.controller.kafkaStreamExample.systemCommandRunnerForShellScript --num-executors 1 --executor-cores 2 --executor-memory 1g --driver-memory 1g --driver-cores 1 $jarName scriptsNamesWithParam=$scriptsNamesWithParam


#sh /home/raptor/IdeaProjects/SparkLearning/kafkaStreamingPipelineScripts/scriptTriggeringScript.sh startExtractLoadSteam.sh~CarSensor

#sh /home/raptor/IdeaProjects/SparkLearning/kafkaStreamingPipelineScripts/scriptTriggeringScript.sh startReadStream.sh
