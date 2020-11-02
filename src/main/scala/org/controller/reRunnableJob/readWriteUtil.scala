package  org.controller.reRunnableJob

import  org.controller.reRunnableJob.constants._
import org.apache.spark.sql.{DataFrame, SparkSession}
import java.io.InputStreamReader
import java.net.URI
import javax.activation.DataHandler
import javax.mail.internet.{InternetAddress, MimeBodyPart, MimeMessage, MimeMultipart}
import javax.mail.util.ByteArrayDataSource
import javax.mail._
import java.io.PrintWriter
import scala.util.Try
import org.apache.hadoop.conf.Configuration
import java.util.Properties
import org.apache.hadoop.fs.{Path ,FSDataOutputStream,FileSystem}

object readWriteUtil {
  def wgetRunner(inputMap:collection.mutable.Map[String,String]) ={
    import sys.process._
    var output_stats = -1
    try{
      Try(inputMap(wgetFileNameArg)).isSuccess match {
        case true => output_stats=s"wget -O ${inputMap(wgetFileNameArg)} ${inputMap(wgetHttpPathArg)} "!
        case _ => output_stats= s"wget ${inputMap(wgetHttpPathArg)} "!
      }
    }
    catch
      {
        case e:Exception => println("Exception occurred while downloading file")
      }
    output_stats match
    {
      case value if value ==0 => true
      case _ => false
    }
  }

  def readDF(spark:SparkSession,inputMap:collection.mutable.Map[String,String])={
    import spark.implicits._
    var df:DataFrame=null
    try{
      inputMap(fileFormatArg) match
      {
        case value if value == csvFileFormatArg => df =spark.read.format(dataBricksCSVformat).option(headerOption,inputMap(headerOption)).option(inferSchemaOption,inputMap(inferSchemaOption)).option(delimiterOption,inputMap(delimiterOption)).csv(inputMap(pathOption))
        case value if value == jdbcFormat => df =spark.read.format(jdbcFormat).option(driverOption, inputMap(driverOption)).option(urlOption, inputMap(urlOption)).option(dbTableOption, inputMap(dbTableOption)).option(userOption, inputMap(userOption)).option(passwordOption, inputMap(passwordOption)).load
        case value if value == jsonFileFormatArg => df =spark.read.json(inputMap(pathOption))
        case value if value == parquetFileFormatArg => df =spark.read.load(inputMap(pathOption))
      }
    }
    catch
      {case e:Exception=> println(s"error while reading the source \n ${e.printStackTrace}")}
    df
  }

  def writeDF(spark:SparkSession,inputMap:collection.mutable.Map[String,String],dataFrame: DataFrame)={
    inputMap(fileFormatArg) match {
      case value if value == jsonFileFormatArg => dataFrame.repartition(Try{inputMap(DFrepartitionArg).toInt}.isSuccess match {case true => inputMap(DFrepartitionArg).toInt case _ => 100}).coalesce(Try{inputMap(coalesceArg).toInt}.isSuccess match {case true => inputMap(coalesceArg).toInt case _ => 1}).write.mode(inputMap(saveModeArg)).json(inputMap(pathOption))
      case value if value == csvFileFormatArg => dataFrame.repartition(Try{inputMap(DFrepartitionArg).toInt}.isSuccess match {case true => inputMap(DFrepartitionArg).toInt case _ => 100}).coalesce(Try{inputMap(coalesceArg).toInt}.isSuccess match {case true => inputMap(coalesceArg).toInt case _ => 1}).write.mode(inputMap(saveModeArg)).option(delimiterOption,inputMap(delimiterOption)).option(headerOption,inputMap(headerOption)).csv(inputMap(pathOption))
      case value if value == parquetFileFormatArg => dataFrame.repartition(Try{inputMap(DFrepartitionArg).toInt}.isSuccess match {case true => inputMap(DFrepartitionArg).toInt case _ => 100}).coalesce(Try{inputMap(coalesceArg).toInt}.isSuccess match {case true => inputMap(coalesceArg).toInt case _ => 1}).write.mode(inputMap(saveModeArg)).save(inputMap(pathOption))
    }
  }

  def execSparkSql(spark:SparkSession,inputMap:collection.mutable.Map[String,String])=
  {
    var df:DataFrame=null
    try {
      df=spark.sql(inputMap(sqlStringArg))
    }
    catch
      {
        case e:Exception=> println(s"Error in executing the sql ------> \n ${e.printStackTrace}")
      }
    df
  }

  def insertIntoHive(spark:SparkSession,inputMap:collection.mutable.Map[String,String],dataFrame:DataFrame)= {
    try {
      dataFrame.repartition(Try {
        inputMap(DFrepartitionArg).toInt
      }.isSuccess match { case true => inputMap(DFrepartitionArg).toInt
      case _ => 100
      }).write.mode(inputMap(saveModeArg)).insertInto(inputMap(tableNameArg))
    }
    catch
      {
        case e:Exception => println(s"Error while writing into table -- > \n ${e.printStackTrace}")
      }
  }

  def saveAsTableHive(spark:SparkSession,inputMap:collection.mutable.Map[String,String],dataFrame:DataFrame)= {
    try {
      dataFrame.repartition(Try {
        inputMap(DFrepartitionArg).toInt
      }.isSuccess match { case true => inputMap(DFrepartitionArg).toInt
      case _ => 100
      }).write.mode(inputMap(saveModeArg)).saveAsTable(inputMap(tableNameArg))
    }
    catch
      {
        case e:Exception => println(s"Failed while saving the DF as table ----> \n ${e.printStackTrace}")
      }
  }

  def writerObjectCreator(hdfsDomain:String,filePath:String) =
  {
    val conf = new Configuration()
    conf.set(fsDefaultFS, hdfsDomain)
    val fs= FileSystem.get(conf)
    val output = fs.create(new Path(filePath))
    var writer:PrintWriter=null
    try{
      writer = new PrintWriter(output)
    }
    catch
      {
        case e:Exception => {println(s"Not able to create the property file in this path - ${filePath}") ;e.printStackTrace()}
      }
    writer
  }
  def loadProperties(propPath: String):Properties= {
    val prop = new Properties
    val fs = FileSystem.get(new Configuration)
    try {
      val hdfsPath = new Path(propPath)
      val fis = new InputStreamReader(fs.open(hdfsPath))
      prop.load(fis)
    } catch {
      case ex: Exception => println(s"Properties file ${propPath} doesn't exists (or) error in accessing it \n ${ex.printStackTrace}")
    }
    prop
  }

  // reads all files inside the folder and subfolder and returns as lid\st buffer
  val fileArrayBuffer=new collection.mutable.ArrayBuffer[String]
  def getAllFilePath(filePath:Path , fs:FileSystem ):collection.mutable.ArrayBuffer[String]=
  {
    val fileStatus = fs.listStatus(filePath)
    for (fileStat <- fileStatus)
      if (fileStat.isDirectory()) // use isDir inside RDP
        getAllFilePath(fileStat.getPath(), fs)
      else
        fileArrayBuffer+=fileStat.getPath().toString()
    fileArrayBuffer
  }

  def mainSenderWithAttachment(inputMap:collection.mutable.Map[String,String])={
    val toEmail=inputMap(toEmailProperty)
    val fromEmail=inputMap(fromEmailProperty)
    val password=inputMap(fromPasswordProperty)
    val properties = System.getProperties()
    properties.put(mailSMPTHostProp, smtpHost)
    properties.put(mailSMTPAuthProp, stringTrue)
    properties.put(mailSMPTStarttlsEnableProp, stringTrue)
    properties.put(mailSMTPPortProp, outlookSMTPPort)
    val session = Session.getDefaultInstance(properties,null)
    val message = new MimeMessage(session)
    message.setFrom(new InternetAddress(fromEmail))
    message.setSubject(inputMap(messageSubjectArg))
    val toMailIdList=toEmail.split(",").map(_.toString).toList
    for (toMailId <-toMailIdList)
      message.addRecipient(Message.RecipientType.TO,new InternetAddress(toMailId))
    val messageBodyPart = new MimeBodyPart()
    messageBodyPart.setText(inputMap(messageContentArg))
    val namenode = inputMap(hdfsDomain)
    val hdfs = FileSystem.get(new URI(namenode), new Configuration())
    val path = new Path(inputMap(filePathArg))
    val stream = hdfs.open(path)
    val messageBodyPartFile = new MimeBodyPart()
    messageBodyPartFile.setDataHandler(new DataHandler(new ByteArrayDataSource(stream,"text/csv")))
    messageBodyPartFile.setFileName(inputMap(attachmentNameArg))
    val multipart = new MimeMultipart()
    multipart.addBodyPart(messageBodyPart)
    multipart.addBodyPart(messageBodyPartFile)
    message.setContent(multipart)
    val transport=session.getTransport(smtpHost)
    try {
      transport.connect(outlookSMTPHost, fromEmail, password)
      transport.sendMessage(message, message.getAllRecipients)
    }
    catch
      {
        case e:Exception => println("Error while getting Auth for sending mail")
      }
  }
  def mainSender(inputMap:collection.mutable.Map[String,String])={
    val toEmail=inputMap(toEmailProperty)
    val fromEmail=inputMap(fromEmailProperty)
    val password=inputMap(fromPasswordProperty)
    val properties = System.getProperties()
    properties.put(mailSMPTHostProp, smtpHost)
    properties.put(mailSMTPAuthProp, stringTrue)
    properties.put(mailSMPTStarttlsEnableProp, stringTrue)
    properties.put(mailSMTPPortProp, outlookSMTPPort)
    val session = Session.getDefaultInstance(properties,null)
    val message = new MimeMessage(session)
    message.setFrom(new InternetAddress(fromEmail))
    message.setSubject(inputMap(messageSubjectArg))
    val toMailIdList=toEmail.split(",").map(_.toString).toList
    for (toMailId <-toMailIdList)
      message.addRecipient(Message.RecipientType.TO,new InternetAddress(toMailId))
    val messageBodyPart = new MimeBodyPart()
    messageBodyPart.setText(inputMap(messageContentArg))
    val multipart = new MimeMultipart()
    multipart.addBodyPart(messageBodyPart)
    message.setContent(multipart)
    val transport=session.getTransport(smtpHost)
    try {
      transport.connect(outlookSMTPHost, fromEmail, password)
      transport.sendMessage(message, message.getAllRecipients)
    }
    catch
      {
        case e:Exception => println(s"Error while getting Auth for sending mail ${e.printStackTrace}")
      }
  }
  def fileOutputStreamObjectCreator(hdfsDomain:String,filePath:String) =
  {
    val conf = new Configuration()
    conf.set("fs.defaultFS", hdfsDomain) //comment for databricks
    val fs= FileSystem.get(conf)
    var fileOutputStream:FSDataOutputStream =null
    try
    {
      fs.exists(new Path(filePath)) match {
        case true => try {
          println(s"File already exists - ${filePath}")
          println(s"Appender object will be created for - ${filePath}")
          fileOutputStream=fs.append(new Path(filePath))
        }
        catch
          {
            case e:Exception => {println(s"Not able to open file in this path - ${filePath}") ;e.printStackTrace()}
          }
        case false => try {
          println(s"File created - ${filePath}")
          println(s"Writer object will be created for - ${filePath}")
          fileOutputStream=fs.create(new Path(filePath))
        }
        catch
          {
            case e:Exception => {println(s"Not able to create file in this path - ${filePath}") ;e.printStackTrace()}
          }
      }
    }
    catch
      {
        case e:Exception => {println(s"Not able to access this path - ${filePath} \n might be an access issue") ;e.printStackTrace()}
      }
    fileOutputStream
  }

}
