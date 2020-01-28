#appending data to Bronze table with new schema    
cd /home/raptor/IdeaProjects/SparkLearning/build/libs 

spark-submit --class org.controller.deltaLakeEG.mergingSchemaNewColumnInBronze --deploy-mode client --master yarn --num-executors 1 --executor-memory 1g --executor-cores 2 --driver-memory 1g --driver-cores 1  --packages io.delta:delta-core_2.11:0.5.0 SparkLearning-1.0-SNAPSHOT.jar mode=append createOrAppendForDeltaWrite=append basePath=hdfs://localhost/user/raptor/testing/hadoop/deltaTableTestFolder/ deltaTableBaseName=carDetailTable deltaTableType=Bronze selectExprNeeded=Yes fileName=Avail_car_ExtraColumn_schema.txt mergeSchemaNeeded=Yes
