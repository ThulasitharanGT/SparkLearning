echo '-----------------type 244 an then execute this shell script to select 2.4.4 version-----------'
echo '-----------------'
PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin
echo '-----------------'
echo $SPARK_HOME
echo '-----------------'
echo $PATH

cd /home/raptor/IdeaProjects/KafkaGradleTest/build/libs/

spark-submit --class com.test.AtomicityUsingMysql.testScalaSparkKafkaRead --deploy-mode client --master yarn --num-executors 1 --executor-memory 1g --executor-cores 2 --driver-memory 1g --driver-cores 1  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:2.4.4,com.fasterxml.jackson.core:jackson-databind:2.10.1,com.fasterxml.jackson.core:jackson-core:2.10.1,org.codehaus.jackson:jackson-core-asl:1.9.13,org.codehaus.jackson:jackson-mapper-asl:1.9.13,com.fasterxml.jackson.core:jackson-annotations:2.10.1,com.fasterxml.jackson.dataformat:jackson-dataformat-yaml:2.10.1,com.fasterxml.jackson.module:jackson-module-scala_2.12:2.10.1,com.fasterxml.jackson.module:jackson-module-jaxb-annotations:2.10.1,org.json4s:json4s-jackson_2.12:3.5.3,com.twitter:parquet-jackson:1.6.0,org.codehaus.jackson:jackson-jaxrs:1.9.13,org.codehaus.jackson:jackson-xc:1.9.13,com.fasterxml.jackson.module:jackson-module-paranamer:2.10.1,com.google.protobuf:protobuf-java:3.11.1,org.apache.htrace:htrace-core:3.1.0-incubating,commons-cli:commons-cli:1.4  KafkaGradleTest-1.0-SNAPSHOT-all.jar
