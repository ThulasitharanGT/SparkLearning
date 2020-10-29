echo '-----------------type 240 an then execute this shell script to select 2.4.0 version (defaullt set to 2.4.0 in machine)-----------'
PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin
echo '-----------------'
echo $SPARK_HOME
echo '-----------------'
echo $PATH

cd /home/raptor/IdeaProjects/SparkLearning/build/libs

spark-submit --class org.controller.hbaseSpark.hbaseSparkReadWrite --deploy-mode client --master yarn --num-executors 1 --executor-memory 1g --executor-cores 2 --driver-memory 1g --driver-cores 1   --packages com.hortonworks:shc-core:1.1.1-2.1-s_2.11,com.hortonworks:shc:1.1.1-2.1-s_2.11,org.apache.hbase:hbase-client:1.2.5,org.apache.hbase:hbase-server:1.2.5,org.apache.hbase:hbase-common:1.2.5,org.apache.hbase:hbase-protocol:1.2.5,org.apache.hbase:hbase-hadoop2-compat:1.2.5,org.apache.hbase:hbase-annotations:1.2.5 --repositories https://repository.apache.org/content/repositories/releases SparkLearning-1.0-SNAPSHOT.jar

