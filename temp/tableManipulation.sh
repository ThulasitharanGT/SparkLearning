#appending data to Bronze table

#selectExprNeeded=Yes
#fileName=Avail_car4.txt
#deltaTableType=Bronze
#createOrAppendOrOverwriteForDeltaWrite=append
#mode=append
mode=$1
createOrAppendOrOverwriteForDeltaWrite=$2
deltaTableType=$3
dateConversionNeeded=$4
fileName=$5
mergeSchemaNeeded=$6

cd /home/raptor/IdeaProjects/SparkLearning/build/libs


spark-submit --class org.controller.deltaLakeEG.deltaHadoopJobTest --deploy-mode client --master yarn --num-executors 1 --executor-memory 1g --executor-cores 2 --driver-memory 1g --driver-cores 1  --packages io.delta:delta-core_2.11:0.5.0 SparkLearning-1.0-SNAPSHOT.jar mode=$mode createOrAppendOrOverwriteForDeltaWrite=$createOrAppendOrOverwriteForDeltaWrite basePath=hdfs://localhost/user/raptor/testing/hadoop/deltaTableTestFolder/ deltaTableBaseName=carDetailTable deltaTableType=$deltaTableType dateConversionNeeded=$dateConversionNeeded fileName=$fileName mergeSchemaNeeded=$mergeSchemaNeeded

# creation bronze
#sh /home/raptor/IdeaProjects/SparkLearning/temp/tableManipulation.sh append create Bronze no Avail_car6.txt no
#tableManipulation.sh~append~create~Bronze~no~Avail_car6.txt~no
# append 1 bronze
#sh /home/raptor/IdeaProjects/SparkLearning/temp/tableManipulation.sh append append Bronze Yes Avail_car4.txt no
#tableManipulation.sh~append~append~Bronze~Yes~Avail_car4.txt~no
# append 2 bronze
#sh /home/raptor/IdeaProjects/SparkLearning/temp/tableManipulation.sh append append Bronze No Avail_car2.txt no
#tableManipulation.sh~append~append~Bronze~No~Avail_car2.txt~no
# merge schema  bronze (updation of schema)
#sh /home/raptor/IdeaProjects/SparkLearning/temp/tableManipulation.sh append append Bronze No Avail_car5.txt yes
#tableManipulation.sh~append~append~Bronze~No~Avail_car5.txt~yes
# merge schema  bronze (updation of schema)
#sh /home/raptor/IdeaProjects/SparkLearning/temp/tableManipulation.sh append append Bronze No Avail_car_ExtraColumn_schema.txt yes
#tableManipulation.sh~append~append~Bronze~No~Avail_car_ExtraColumn_schema.txt~yes
# creation silver
#sh /home/raptor/IdeaProjects/SparkLearning/temp/tableManipulation.sh append create Silver No Avail_car6.txt no
#tableManipulation.sh~append~create~Silver~No~Avail_car6.txt~no

#tableManipulation.sh~append~create~Bronze~no~Avail_car6.txt~no,tableManipulation.sh~append~append~Bronze~Yes~Avail_car4.txt~no,tableManipulation.sh~append~append~Bronze~No~Avail_car2.txt~no,tableManipulation.sh~append~append~Bronze~No~Avail_car5.txt~yes,tableManipulation.sh~append~append~Bronze~No~Avail_car_ExtraColumn_schema.txt~yes,tableManipulation.sh~append~create~Silver~No~Avail_car6.txt~no,tableAppendingSilver_1.sh