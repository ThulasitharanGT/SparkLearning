packagesList=$1
jarName=$2
basePath=$3
scriptNameList=$4

echo "executing "$0

#jarName=SparkLearning-1.0-SNAPSHOT.jar
#basePath=/home/raptor/IdeaProjects/SparkLearning/temp/
#scriptNameList=tableCreationBronze.sh,tableAppendingBronze_1.sh,tableAppendingBronze_2.sh,tableAppendingBronze_3.sh,tableUpdationBronze_1.sh,tableUpdationBronze_2.sh,tableCreationSilver.sh,tableAppendingSilver_1.sh
#packagesList=io.delta:delta-core_2.11:0.5.0

cd /home/raptor/IdeaProjects/SparkLearning/build/libs

spark-submit --class org.controller.deltaLakeEG.shellScriptSystemCommand --deploy-mode client --master yarn --num-executors 1 --executor-memory 1g --executor-cores 2 --driver-memory 1g --driver-cores 1  --packages $packagesList $jarName basePath=$basePath scriptNameList=$scriptNameList
#old
#sh /home/raptor/IdeaProjects/SparkLearning/temp/pipeLineTriggerScript.sh  io.delta:delta-core_2.11:0.5.0 SparkLearning-1.0-SNAPSHOT.jar /home/raptor/IdeaProjects/SparkLearning/temp/ tableCreationBronze.sh,tableAppendingBronze_1.sh,tableAppendingBronze_2.sh,tableAppendingBronze_3.sh,tableUpdationBronze_1.sh,tableUpdationBronze_2.sh,tableCreationSilver.sh,tableAppendingSilver_1.sh
#new
#sh /home/raptor/IdeaProjects/SparkLearning/temp/pipeLineTriggerScript.sh  io.delta:delta-core_2.11:0.5.0 SparkLearning-1.0-SNAPSHOT.jar /home/raptor/IdeaProjects/SparkLearning/temp/ tableManipulation.sh~append~create~Bronze~no~Avail_car6.txt~no,tableManipulation.sh~append~append~Bronze~Yes~Avail_car4.txt~no,tableManipulation.sh~append~append~Bronze~No~Avail_car2.txt~no,tableManipulation.sh~append~append~Bronze~No~Avail_car5.txt~yes,tableManipulation.sh~append~append~Bronze~No~Avail_car_ExtraColumn_schema.txt~yes,tableManipulation.sh~append~create~Silver~No~Avail_car6.txt~no,tableAppendingSilver_1.sh