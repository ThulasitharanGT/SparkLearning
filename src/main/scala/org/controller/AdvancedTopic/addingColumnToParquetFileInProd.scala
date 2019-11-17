package org.controller.AdvancedTopic
import org.util.SparkOpener
import org.apache.spark.sql.functions._
import sys.process._
import scala.collection.mutable.ListBuffer

object addingColumnToParquetFileInProd extends SparkOpener {

  val spark=SparkSessionLoc("temp")
  def main(args: Array[String]): Unit = {


  //// comand to add column and rename
  val cred="-Dfs.s3a.access.key=AKIAIA6BZFLJIF7PBAOA -Dfs.s3a.secret.key=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 -Dfs.s3a.proxy.host=proxyanbcge.nbc.com -Dfs.s3a.proxy.port=80"
  var filePathStringListFinal=new ListBuffer[String]()
  val command1= "hdfs dfs -rm -r /data/prod_nfr_ext/temp_folder_2" // deleeting folder in hdfs -- > due to proxy issue
  command1!
  val command2="hdfs dfs -mkdir -p /data/prod_nfr_ext/temp_folder_2/" // creating folder in hdfs -- > due to proxy issue
  command2!
  val filePathList=List("hdfs dfs "+cred+" -ls /data/prod_nfr_ext/temp_folder_2/pacing/*/*/*/*","hdfs dfs "+cred+" -ls /data/prod_nfr_ext/temp_folder_2/cycles/*/*/*/*/*","hdfs dfs "+cred+" -ls /data/prod_nfr_ext/temp_folder_2/adhoc/*/*/*/*")
  val listPaths=List("adhoc","pacing","cycles")
  for (path <- listPaths) // copying from s3  -- > due to proxy issue
    "hadoop distcp "+cred+" s3a://nbcu-msi-compass/compass_extract/raw/"+path+" /data/prod_nfr_ext/temp_folder_2/"!
  var filePathStringList=List[String]()
  for(fp <- filePathList )
  {
    val filePathString=fp!!
    val filePathList=filePathString.split("\n")
    filePathStringList=filePathStringList++filePathList
  }
  for (i <- filePathStringList) // taking paths alone
    if (i.contains("/"))
      filePathStringListFinal+=i

  val filePathStringStringTemp=filePathStringListFinal.map(value => value.substring(value.indexOf("/"),value.length)) // removing permitons, replication factor etc
  val src="/data/prod_nfr_ext/temp_folder_2/"  // HDFS path in list
  val c1 ="mkdir -p /data/dev/bedrock/solution/datafeed/nov17_1100/bin/Compass_extract_dev_test/Parquest_testing/temp_folder_prod/" // newly created path
    c1!
  val c2 ="hdfs dfs -get /data/prod_nfr_ext/temp_folder_2/*  /data/dev/bedrock/solution/datafeed/nov17_1100/bin/Compass_extract_dev_test/Parquest_testing/temp_folder_prod/" // moving hdfs data to local --> proxy issue
    c2!
  val dest="/data/dev/bedrock/solution/datafeed/nov17_1100/bin/Compass_extract_dev_test/Parquest_testing/temp_folder_prod/" // local path
  for (path <- filePathStringStringTemp)
  {
    val inPath="file://"+path.replace(src,dest)  // changing hdfs to local path
  val outPath=inPath.replace("temp_folder_prod","temp_Finalcols_try")   // changing  local output path
    if(path.contains(".parquet"))
    {
      val df=spark.read.load(inPath)
      val UpdatedDF=df.withColumn("SubnetBlockFlag",lit(" "))//.withColumn("LockedFlag",lit(" ")).withColumn("titleversionno",lit(" "))
      UpdatedDF.coalesce(1).write.mode("overwrite").format("parquet").save(outPath.substring(0,outPath.lastIndexOf("/"))) // beause will write as part file
      println(outPath.substring(0,outPath.lastIndexOf("/")))
    }
  }

  for (path <- filePathStringStringTemp) // renaming
  {
    val inPath=path.replace(src,dest)
    val outPath=inPath.replace("temp_folder_prod","temp_Finalcols_try")
    val rmSuccessCommand="rm "+outPath.substring(0,outPath.lastIndexOf("/"))+"/_SUCCESS"
    rmSuccessCommand!
    val lsCommand="ls "+outPath.substring(0,outPath.lastIndexOf("/"))+"/ "  // taking path till before of the part file
  val fileName=lsCommand!! // listing and getting the name of part file
  val fileNameFinal=fileName.split("\n") // taking the name of part file as string
  val mvCommand="mv "+outPath.substring(0,outPath.lastIndexOf("/"))+"/"+fileNameFinal(0)+" "+outPath // renaming command from part file to required file  name
    mvCommand!
  }
    //to test

    val totFile=spark.read.load("/data/dev/bedrock/solution/datafeed/nov17_1100/bin/Compass_extract_dev_test/Parquest_testing/temp_Finalcols_try/adhoc/*/*/*/*","/data/dev/bedrock/solution/datafeed/nov17_1100/bin/Compass_extract_dev_test/Parquest_testing/temp_Finalcols_try/cycles/*/*/*/*/*","/data/dev/bedrock/solution/datafeed/nov17_1100/bin/Compass_extract_dev_test/Parquest_testing/temp_Finalcols_try/pacing/*/*/*/*")
    totFile.printSchema
  }

}
  // with s3 full access
  /*
  import sys.process._
  import scala.collection.mutable.ListBuffer
  val cred="-Dfs.s3a.access.key=AKIAIA6BZFLJIF7PBAOA -Dfs.s3a.secret.key=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 -Dfs.s3a.proxy.host=proxyanbcge.nbc.com -Dfs.s3a.proxy.port=80" // pproxy must be set in emr by defaault
  var filePathStringListFinal=new ListBuffer[String]()
  "hdfs dfs "+cred+" -rm -r s3a://nbcu-msi-compass/tempCopy/data/prod_nfr_ext/temp_folder_2"!
    "hdfs dfs "+cred+" -mkdir -p s3a://nbcu-msi-compass/tempCopy/data/prod_nfr_ext/temp_folder_2/"!
  val filePathList=List("hdfs dfs "+cred+" -ls s3a://nbcu-msi-compass/tempCopy/data/prod_nfr_ext/temp_folder_2/pacing/*/*/*/*","hdfs dfs "+cred+" -ls s3a://nbcu-msi-compass/tempCopy/data/prod_nfr_ext/temp_folder_2/cycles/*/*/*/*/*","hdfs dfs "+cred+" -ls s3a://nbcu-msi-compass/tempCopy/data/prod_nfr_ext/temp_folder_2/adhoc/*/*/*/*")
  val listPaths=List("adhoc","pacing","cycles")
  for (path <- listPaths)
    "hadoop distcp "+cred+" s3a://nbcu-msi-compass/compass_extract/raw/"+path+" s3a://nbcu-msi-compass/tempCopy/data/prod_nfr_ext/temp_folder_2/"!
  var filePathStringList=List[String]()
  for(fp <- filePathList )
  {
    val filePathString=fp!!
    val filePathList=filePathString.split("\n")
    filePathStringList=filePathStringList++filePathList
  }
  for (i <- filePathStringList) // taking paths alone
    if (i.contains("s3a"))
      filePathStringListFinal+=i

  val filePathStringStringTemp=filePathStringListFinal.map(value => value.substring(value.indexOf("s"),value.length)) // removing permitons, replication factor etc
  for (path <- filePathStringStringTemp)
  {
    val inPath="file://"+path
    val outPath=inPath.replace("temp_folder_2","temp_Finalcols_try")   // changing  local output path
    if(path.contains(".parquet"))
    {
      val df=spark.read.load(inPath)
      val UpdatedDF=df.withColumn("SubnetBlockFlag",lit(" "))//.withColumn("LockedFlag",lit(" ")).withColumn("titleversionno",lit(" "))
      UpdatedDF.coalesce(1).write.mode("overwrite").format("parquet").save(outPath.substring(0,outPath.lastIndexOf("/"))) // beause will write as part file
      println(outPath.substring(0,outPath.lastIndexOf("/")))
    }
  }

  for (path <- filePathStringStringTemp) // renaming
  {
    val inPath=path
    val outPath=inPath.replace("temp_folder_2","temp_Finalcols_try")
    val rmSuccessCommand="rm "+outPath.substring(0,outPath.lastIndexOf("/"))+"/_SUCCESS"
    rmSuccessCommand!
    val lsCommand="ls "+outPath.substring(0,outPath.lastIndexOf("/"))+"/ "  // taking path till before of the part file
  val fileName=lsCommand!! // listing and getting the name of part file
  val fileNameFinal=fileName.split("\n") // taking the name of part file as string
  val mvCommand="mv "+outPath.substring(0,outPath.lastIndexOf("/"))+"/"+fileNameFinal(0)+" "+outPath // renaming command from part file to required file  name
    mvCommand!
  }

*/


