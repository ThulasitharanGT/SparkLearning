package temp

import org.apache.spark.sql._
import org.apache.spark.SparkFiles
import org.apache.spark.sql.types._
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions._

import java.sql.Timestamp
import java.util.{ Calendar, Locale, TimeZone }
import java.text.SimpleDateFormat
import java.util.Date
import java.util.Calendar
import scala.io.Source

import org.apache.log4j._
import org.apache.log4j.Logger
import org.apache.log4j.LogManager;
import org.apache.log4j.PropertyConfigurator

import java.sql.{Connection, Driver, DriverManager, JDBCType, PreparedStatement, ResultSet, ResultSetMetaData, SQLException}

import java.util.Properties

object testScalaObj {

  //SAILFISH DB access credentials
  val dbDetails = "jdbc:db2://edcaiasd300:50000/bludb,gdp5u1a,svc-hdppvdu,kZy58LO64acYzqs,gda5u1a,jdbc:db2://imdsstg01:62033/edw5s1,etldbc,p1ttym3,uat5s1,9/1/2010,yes"
  val targetDBDetils = "jdbc:db2://edcaiasd300:50000/bludb,gdp5u1a,svc-hdppvdu,kZy58LO64acYzqs"
  //val dbDetails = "jdbc:db2://edcaiasd300:50000/bludb,gdp5u1a,J8MX,Ch420840,jdbc:db2://imdsprd01:60354/edw5p1,gda5u1a"
  val slfshConnect = dbDetails.split(",")(0)
  val slfshSchema = dbDetails.split(",")(1)
  val user = dbDetails.split(",")(2)
  val password = dbDetails.split(",")(3)
  val dmgcSchema = dbDetails.split(",")(4)
  val sdwConnect = dbDetails.split(",")(5)
  val sdwUser =  dbDetails.split(",")(6)
  val sdwPassword = dbDetails.split(",")(7)
  val sdwSchema = dbDetails.split(",")(8)
  val prevCutoff = dbDetails.split(",")(9)
  val ongoing = dbDetails.split(",")(10)

  //sailfish credentials
  val driverName = "com.ibm.db2.jcc.DB2Driver"
  val prop = new Properties()
  prop.put("user", user)
  prop.put("password", password)
  prop.put("driver", driverName)

  //SDW credentials
  val sdwProp = new Properties()
  sdwProp.put("user", sdwUser)
  sdwProp.put("password", sdwPassword)
  sdwProp.put("driver", driverName)
  val startJob = new Timestamp (System.currentTimeMillis())
  val curr_dttm = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSSSS").format(Calendar.getInstance.getTime())
  def main(args: Array[String]): Unit = {
    //get current date for the logic
    val current_date = java.time.LocalDate.now.toString()
    val dttm = java.time.LocalDate.now.minusDays(180).toString()
    println("Current Date: " + current_date + " Dttm: " + dttm)

    val spark = SparkSession.builder().appName("pvdr_crdg_dtl").getOrCreate()
      //.config("spark.master", "local")
      //Provider
      //PVDR Table (DRIVING SOURCE)

    var pvdrStr = s"""(SELECT TNNT_ID, PVDR_ID, CRED_MNL_CD, PVDR_TYPE_NM, PVDR_DTL_TYPE_NM FROM ${slfshSchema}.PVDR
                       WHERE ((PVDR_TYPE_NM = 'Organization' AND PVDR_DTL_TYPE_NM IN ('Facility', 'Supplier Business','Group','Mobile'))
                              			OR (PVDR_TYPE_NM = 'Individual' AND PVDR_DTL_TYPE_NM IN ('Professional','Supplier Individual'))))"""
    val pvdrDF = spark.read.jdbc(slfshConnect,pvdrStr,prop)
    println("pvdrDF Count: "+pvdrDF.count())
  //  val result_pvdrDF=functionToCalculate_CRDG_RQD_CD(spark,pvdrDF)
    pvdrDF.createOrReplaceTempView("pvdrTmpTbl")
    // PVDR_IRNL_ID_XREF
    var piicrStr = s"""(SELECT PVDR_ID, PVDR_ALTT_ID, ALTT_ID_TYPE_CD FROM ${slfshSchema}.PVDR_IRNL_ID_XREF where ALTT_ID_TYPE_CD = 'PIMS')"""
    val piicrDF = spark.read.jdbc(slfshConnect,piicrStr,prop)
    val PVDR_CTC_COVA_LN_String=s"select * from ${slfshSchema}.PVDR_CTC_COVA_LN where COVA_LN_EPRN_DT > ${curr_dttm} "
    val pcclDF = spark.read.jdbc(slfshConnect,PVDR_CTC_COVA_LN_String,prop)

    var pctcSqlStr = s"""(SELECT CTC_ID, CTC_EPRN_DT
  	                       FROM gdp5u1b.PVDR_CTC
  	                       WHERE CTC_EPRN_DT > '${prevCutoff}')"""
    val pctcDF = spark.read.jdbc(slfshConnect,pctcSqlStr,prop)

  }
  def functionToCalculate_CRDG_RQD_CD(spark:SparkSession,pvdrDF:DataFrame,pcclDF:DataFrame,pctcDF:DataFrame):DataFrame =
    {
      val result_1=pvdrDF.selectExpr("PVDR_ID","case when PVDR_DTL_TYPE_NM ='Group' then 'No' else NULL end as CRDG_RQD_CD")
      val PVDR_TO_PVDR_ASCN_String=s"select * from ${slfshSchema}.PVDR_TO_PVDR_ASCN where  PVDR_ASCN_TYPE_CD in ('Emp','Cont') and PVDR_ASCN_EPRN_DT > ${curr_dttm}"
      val PVDR_TO_PVDR_ASCN_DF=spark.read.jdbc(slfshConnect,PVDR_TO_PVDR_ASCN_String,prop)
      val p2pDF=pvdrDF.where("PVDR_DTL_TYPE_NM = 'Supplier Business'").as("pvdrDF").join(PVDR_TO_PVDR_ASCN_DF.as("PVDR_TO_PVDR_ASCN_DF"),pvdrDF.col("PVDR_ID") === PVDR_TO_PVDR_ASCN_DF.col("RLTD_PVDR_ID")).selectExpr("pvdrDF.PVDR_ID as PVDR_ID","PVDR_TO_PVDR_ASCN_DF.PVDR_ID as PVDR_ID_p2p","RLTD_PVDR_ID")
      val PVDR_LOCN_NETW_GRPG_String=s"select * from ${slfshSchema}.PVDR_LOCN_NETW_GRPG  "
      val PVDR_LOCN_NETW_GRPG_DF=spark.read.jdbc(slfshConnect,PVDR_LOCN_NETW_GRPG_String,prop)
      val plngDF=p2pDF.as("p2pDF").join(PVDR_LOCN_NETW_GRPG_DF.as("PVDR_LOCN_NETW_GRPG_DF"),p2pDF.col("RLTD_PVDR_ID")===PVDR_LOCN_NETW_GRPG_DF.col("PVDR_ID"),"left_outer").selectExpr("p2pDF.PVDR_ID as PVDR_ID","PVDR_LOCN_NETW_GRPG_DF.PVDR_ID as PVDR_ID_plng","SVC_LOCN_ID","PVDR_CTC_ROLE_TYPE_CD")
      val plng2DF=p2pDF.as("p2pDF").join(PVDR_LOCN_NETW_GRPG_DF.as("PVDR_LOCN_NETW_GRPG_DF"),Seq("PVDR_ID"),"left_outer").selectExpr("p2pDF.PVDR_ID as PVDR_ID_2","PVDR_LOCN_NETW_GRPG_DF.PVDR_ID as PVDR_ID_plng2","SVC_LOCN_ID","PVDR_CTC_ROLE_TYPE_CD")
      val result_2=plngDF.join(plng2DF,Seq("SVC_LOCN_ID","PVDR_CTC_ROLE_TYPE_CD")).select("PVDR_ID").withColumn("CRDG_RQD_CD",lit("No"))
      val result_3_a=pvdrDF.filter("PVDR_TYPE_NM = 'Organization' and TAX_ETY_TYPE_CD = 'EINVA'").as("pvdrDF").join(pcclDF.filter("PVDR_CTC_ROLE_TYPE_CD <> 'HBP' and PVDR_NETW_TYPE_CD <> 'FEPD'"),Seq("PVDR_ID"),"inner").withColumn("CRDG_RQD_CD",lit("No"))
      import spark.implicits._
      //val PVDR_CTC_String=s"select * from ${slfshSchema}.PVDR_CTC where COVA_LN_EPRN_DT > ${curr_dttm}"
      //val PVDR_CTC_DF=spark.read.jdbc(slfshConnect,PVDR_CTC_String,prop)

      //var result_3_b:DataFrame=null
      val result_3_b= pvdrDF.join(pctcDF, Seq("CTC_ID"),"inner").select("CNTE_PVDR_ID","CTC_ID").as("PCTC").join(pvdrDF.filter("TAX_ETY_TYPE_CD <> 'EINVA'").as("PVDR2"),$"PCTC.CNTE_PVDR_ID"=== $"PVDR2.PVDR_ID","inner").selectExpr("PVDR2.PVDR_ID as PVDR_ID").withColumn("CRDG_RQD_CD",lit("No"))
      val result_3_c= pvdrDF.join(PVDR_TO_PVDR_ASCN_DF, Seq("PVDR_ID"),"inner").select("PVDR_ID").withColumn("CRDG_RQD_CD",lit("No"))
      val result_3_d=PVDR_TO_PVDR_ASCN_DF.as("ppa").join(pvdrDF.filter("TAX_ETY_TYPE_CD <> 'EINVA'").as("PVDR3"),$"PVDR3.PVDR_ID"===$"RLTD_PVDR_ID","inner").selectExpr("PVDR3.PVDR_ID as PVDR_ID","ppa.RLTD_PVDR_ID as RLTD_PVDR_ID").as("PPA_out").join(pcclDF.as("pccl"),$"PPA_out.RLTD_PVDR_ID"===$"pccl.PVDR_ID","inner").selectExpr("PPA_out.PVDR_ID as PVDR_ID","PPA_out.RLTD_PVDR_ID as RLTD_PVDR_ID").join(pctcDF.filter("PAR_IN = 1").as("PCTC"),Seq("PVDR_ID")).select("PVDR_ID").withColumn("CRDG_RQD_CD",lit("No"))
      val result_3=result_3_a.union(result_3_b).union(result_3_c).union(result_3_d)
      val PVDR_SVC_LOCN_SPLY_String=s"select * from ${slfshSchema}.PVDR_SVC_LOCN_SPLY where SVC_LOCN_SPLY_EPRN_DT > ${curr_dttm}"
      val PVDR_SVC_LOCN_SPLY_DF=spark.read.jdbc(slfshConnect,PVDR_SVC_LOCN_SPLY_String,prop)
      val PROVIDER_SPEC_String=s"select * from ${slfshSchema}.PROVIDER_SPEC"
      val PROVIDER_SPEC_DF=spark.read.jdbc(slfshConnect,PROVIDER_SPEC_String,prop)
      val PROVIDER_VALUE_MAP_String=s"select * from ${slfshSchema}.PROVIDER_VALUE_MAP"
      val PROVIDER_VALUE_MAP_DF=spark.read.jdbc(slfshConnect,PROVIDER_VALUE_MAP_String,prop)
      val windowFunctionVar= Window.partitionBy($"pvdr_id",$"SVC_LOCN_ID").orderBy($"AUD_LAST_UPDT_TM".desc)
      val result_4=pvdrDF.as("PVDR").join(PVDR_SVC_LOCN_SPLY_DF.filter("PVDR_CTC_ROLE_TYPE_CD <> 'HBP'").as("PSLS"),Seq("PVDR_ID"),"inner").select("PVDR.PVDR_ID","PSLS.HLCR_SPLY_TAXN_CD","PSLS.PVDR_CTC_ROLE_TYPE_CD").withColumn("rnum",row_number.over(windowFunctionVar)).filter($"rnum"===1).drop("rnum").as("PSLS_out").join(PROVIDER_SPEC_DF.as("ps"),$"PSLS_out.HLCR_SPLY_TAXN_CD"===$"ps.Code","inner").selectExpr("ps.id as id","PSLS_out.PVDR_ID as PVDR_ID","PSLS_out.PVDR_CTC_ROLE_TYPE_CD as PVDR_CTC_ROLE_TYPE_CD").as("ps_out").join(PROVIDER_VALUE_MAP_DF.filter("MAP_TYPE = 'FB_CVO_EXTRACT_RULE'").as("pvm"),$"pvm.Portico_Value"===$"ps_out.id" && $"ps_out.PVDR_CTC_ROLE_TYPE_CD"===$"pvm.FMG_CODE","inner")
      result_4.union(result_3).union(result_2).union(result_1)
    }
}
