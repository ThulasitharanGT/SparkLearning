#fixing bronze to silver table load with nullvalues in bronze having values in silver

cd /home/raptor/IdeaProjects/SparkLearning/build/libs 

spark-submit --class org.controller.deltaLakeEG.fixBronzeToSilver --deploy-mode client --master yarn --num-executors 1 --executor-memory 1g --executor-cores 2 --driver-memory 1g --driver-cores 1  --packages io.delta:delta-core_2.11:0.5.0 SparkLearning-1.0-SNAPSHOT.jar mode=overwrite basePath=hdfs://localhost/user/raptor/testing/hadoop/deltaTableTestFolder/ deltaTableBaseName=carDetailTable 
