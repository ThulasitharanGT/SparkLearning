package temp

/*
object testScalaObjV2 {

}
*/


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
import org.apache.log4j.LogManager
import org.apache.log4j.PropertyConfigurator

import java.sql.{Connection, Driver, DriverManager, JDBCType, PreparedStatement, ResultSet, ResultSetMetaData, SQLException}

import java.util.Properties;

object testScalaObjV2 {

  def functionToCalculate_CRDG_RQD_CD(spark:SparkSession,pvdrDF:DataFrame,pcclDF:DataFrame,pctcDF:DataFrame,slfshSchema:String, slfshConnect:String, prop:Properties, curr_dttm:String):DataFrame =
  {
    import spark.implicits._

    println("result1")
    val result_1=pvdrDF.selectExpr("PVDR_ID","case when PVDR_DTL_TYPE_NM ='Group' then 'No' else NULL end as CRDG_RQD_CD")
    result_1.printSchema()

    var PVDR_TO_PVDR_ASCN_String=s"""(select * from ${slfshSchema}.PVDR_TO_PVDR_ASCN where PVDR_ASCN_TYPE_CD in ('Emp','Cont') and PVDR_ASCN_EPRN_DT > '${curr_dttm}')"""
    val PVDR_TO_PVDR_ASCN_DF=spark.read.jdbc(slfshConnect,PVDR_TO_PVDR_ASCN_String,prop)

    val p2pDF=pvdrDF.where("PVDR_DTL_TYPE_NM = 'Supplier Business'").as("pvdrDF")
      .join(PVDR_TO_PVDR_ASCN_DF.as("PVDR_TO_PVDR_ASCN_DF"),pvdrDF.col("PVDR_ID") === PVDR_TO_PVDR_ASCN_DF.col("RLTD_PVDR_ID"))
      .selectExpr("pvdrDF.PVDR_ID as PVDR_ID","PVDR_TO_PVDR_ASCN_DF.PVDR_ID as PVDR_ID_p2p","RLTD_PVDR_ID")

    var PVDR_LOCN_NETW_GRPG_String=s"""(select * from ${slfshSchema}.PVDR_LOCN_NETW_GRPG)"""
    val PVDR_LOCN_NETW_GRPG_DF=spark.read.jdbc(slfshConnect,PVDR_LOCN_NETW_GRPG_String,prop)

    val plngDF = p2pDF.as("p2pDF")
      .join(PVDR_LOCN_NETW_GRPG_DF.as("PVDR_LOCN_NETW_GRPG_DF"),p2pDF.col("RLTD_PVDR_ID")===PVDR_LOCN_NETW_GRPG_DF.col("PVDR_ID"),"left_outer")
      .selectExpr("p2pDF.PVDR_ID as PVDR_ID","SVC_LOCN_ID","PVDR_CTC_ROLE_TYPE_CD")

    val plng2DF = p2pDF.as("p2pDF")
      .join(PVDR_LOCN_NETW_GRPG_DF.as("PVDR_LOCN_NETW_GRPG_DF"),Seq("PVDR_ID"),"left_outer")
      .selectExpr("p2pDF.PVDR_ID as PVDR_ID_2","SVC_LOCN_ID","PVDR_CTC_ROLE_TYPE_CD")

    println("result2")
    val result_2=plngDF.join(plng2DF,Seq("SVC_LOCN_ID","PVDR_CTC_ROLE_TYPE_CD"))
      .select("PVDR_ID")
      .withColumn("CRDG_RQD_CD",lit("No"))
    result_2.printSchema()

    val result_3_a = pvdrDF.filter("PVDR_TYPE_NM = 'Organization' and TAX_ETY_TYPE_CD = 'EINVA'").as("pvdrDF")
      .join(pcclDF.filter("PVDR_CTC_ROLE_TYPE_CD <> 'HBP' and PVDR_NETW_TYPE_CD <> 'FEPD'"),Seq("PVDR_ID"),"inner")
      .withColumn("CRDG_RQD_CD",lit("No"))

    val result_3_b = pvdrDF.join(pctcDF, Seq("CTC_ID"),"inner")
      .select("CNTE_PVDR_ID","CTC_ID").as("PCTC")
      .join(pvdrDF.filter("TAX_ETY_TYPE_CD <> 'EINVA'").as("PVDR2"), $"PCTC.CNTE_PVDR_ID"=== $"PVDR2.PVDR_ID","inner")
      .selectExpr("PVDR2.PVDR_ID as PVDR_ID").withColumn("CRDG_RQD_CD",lit("No"))

    val result_3_c = pvdrDF.join(PVDR_TO_PVDR_ASCN_DF, Seq("PVDR_ID"),"inner")
      .select("PVDR_ID")
      .withColumn("CRDG_RQD_CD",lit("No"))

    val result_3_d = PVDR_TO_PVDR_ASCN_DF.as("ppa")
      .join(pvdrDF.filter("TAX_ETY_TYPE_CD <> 'EINVA'").as("PVDR3"),$"PVDR3.PVDR_ID"===$"RLTD_PVDR_ID","inner")
      .selectExpr("PVDR3.PVDR_ID as PVDR_ID","ppa.RLTD_PVDR_ID as RLTD_PVDR_ID").as("PPA_out")
      .join(pcclDF.as("pccl"),$"PPA_out.RLTD_PVDR_ID"===$"pccl.PVDR_ID","inner")
      .selectExpr("PPA_out.PVDR_ID as PVDR_ID","PPA_out.RLTD_PVDR_ID as RLTD_PVDR_ID")
      .join(pctcDF.filter("PAR_IN = 1").as("PCTC"),Seq("PVDR_ID"))
      .select("PVDR_ID")
      .withColumn("CRDG_RQD_CD",lit("No"))

    val result_3=result_3_a.union(result_3_b).union(result_3_c).union(result_3_d)

    var PVDR_SVC_LOCN_SPLY_String=s"""(select * from ${slfshSchema}.PVDR_SVC_LOCN_SPLY where SVC_LOCN_SPLY_EPRN_DT > '${curr_dttm}'"""
    val PVDR_SVC_LOCN_SPLY_DF=spark.read.jdbc(slfshConnect,PVDR_SVC_LOCN_SPLY_String,prop)

    var PROVIDER_SPEC_String=s"""(select * from ${slfshSchema}.PROVIDER_SPEC)"""
    val PROVIDER_SPEC_DF=spark.read.jdbc(slfshConnect,PROVIDER_SPEC_String,prop)


    var PROVIDER_VALUE_MAP_String=s"""(select * from ${slfshSchema}.PROVIDER_VALUE_MAP)"""
    val PROVIDER_VALUE_MAP_DF=spark.read.jdbc(slfshConnect,PROVIDER_VALUE_MAP_String,prop)

    val windowFunctionVar= Window.partitionBy($"pvdr_id",$"SVC_LOCN_ID").orderBy($"AUD_LAST_UPDT_TM".desc)

    val result_4=pvdrDF.as("PVDR")
      .join(PVDR_SVC_LOCN_SPLY_DF.filter("PVDR_CTC_ROLE_TYPE_CD <> 'HBP'").as("PSLS"),Seq("PVDR_ID"),"inner")
      .select("PVDR.PVDR_ID","PSLS.HLCR_SPLY_TAXN_CD","PSLS.PVDR_CTC_ROLE_TYPE_CD")
      .withColumn("rnum",row_number.over(windowFunctionVar)).filter($"rnum"===1).drop("rnum").as("PSLS_out")
      .join(PROVIDER_SPEC_DF.as("ps"),$"PSLS_out.HLCR_SPLY_TAXN_CD"===$"ps.Code","inner")
      .selectExpr("ps.id as id","PSLS_out.PVDR_ID as PVDR_ID","PSLS_out.PVDR_CTC_ROLE_TYPE_CD as PVDR_CTC_ROLE_TYPE_CD").as("ps_out")
      .join(PROVIDER_VALUE_MAP_DF.filter("MAP_TYPE = 'FB_CVO_EXTRACT_RULE'").as("pvm"),$"pvm.Portico_Value"===$"ps_out.id" && $"ps_out.PVDR_CTC_ROLE_TYPE_CD"===$"pvm.FMG_CODE","inner")

    val finalResult = result_1.union(result_2).union(result_3).union(result_4)

    finalResult
  }

  def writeToDatabase(inDf:Dataset[Row],settingsData:String): Int =
  {

    val sfConnect=settingsData.split(",")(0)
    val sfSchema=settingsData.split(",")(1)
    val userNm=settingsData.split(",")(2)
    val userPwd=settingsData.split(",")(3)
    var retCode = 1

    val sqlMerge = "MERGE INTO " + sfSchema + """.PVDR_CRDG_DTL PS
                      USING (VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                                    ? ,? ,?)) AS
                      S(TNNT_ID,
                        PVDR_ID,
                        CRDG_BGN_DT,
                        CRDG_DLG_IN,
                        CRDG_TYPE_CD,
                        CRDG_END_DT,
                        CRDG_STAT_CD,
                        CRDG_NOTE_TX,
                        CRDG_TERM_RSN_CD,
                        CRDG_CMTE_DT,
                        CRDG_NON_RSPR_IN,
                        CRDG_SENT_TO_VNDR_DT,
                        SEND_FOR_CRDG_IN,
                        TERM_RCND_SEND_FOR_CRED_IN,
                        NETW_SUTE_TOOL_EXT_IN_DT,
                        TERM_CTC_EXT_SENT_VNDR_D,
                        SEND_FOR_CRDG_SET_DT,
                        TERM_RCND_SET_DT,
                        CRDG_OVRD_IN,
                        CRDG_RQD_CD,
                        CRED_CD,
                        INCEXT_SENT_DT,
                        INCEXT_IN,
                        PVDR_FLBL_NB,
                        CRDG_CNTC_FRST_NM,
                        CRDG_CNTCT_LAST_NM,
                        EDCN_REQ_SENT_TO_VNDR_DT,
                        MDCD_REQ_SENT_TO_VNDR_DT,
                        MNL_RSEN_IN,
                        MNL_RSEN_DT,
                        MNL_UDA_RMVL_IN,
                        MNL_UDA_RMVL_DT,
                        CR2_RMVL_SENT_DT,
                        AUD_LAST_UPDT_TM,
                        SRC_SYS_CD,
                        LAST_UPDT_SRC_SYS_CD,
                        AUD_LOAD_ID,
                        AUD_LAST_UPDT_ID,
                        SCRY_ROLE_CD,
                        AUD_CTE_TM,
                        EVNT_TM,
                        EVNT_ID,
                        LAST_UPDT_TM)

                      ON (PS.TNNT_ID = S.TNNT_ID AND PS.PVDR_ID = S.PVDR_ID)

                      WHEN MATCHED THEN UPDATE SET
                           PS.CRDG_BGN_DT = S.CRDG_BGN_DT,
                           PS.CRDG_DLG_IN = S.CRDG_DLG_IN,
                           PS.CRDG_TYPE_CD = S.CRDG_TYPE_CD,
                           PS.CRDG_END_DT = S.CRDG_END_DT,
                           PS.CRDG_STAT_CD = S.CRDG_STAT_CD,
                           PS.CRDG_NOTE_TX = S.CRDG_NOTE_TX,
                           PS.CRDG_TERM_RSN_CD = S.CRDG_TERM_RSN_CD,
                           PS.CRDG_CMTE_DT = S.CRDG_CMTE_DT,
                           PS.CRDG_NON_RSPR_IN = S.CRDG_NON_RSPR_IN,
                           PS.CRDG_SENT_TO_VNDR_DT = S.CRDG_SENT_TO_VNDR_DT,
                           PS.SEND_FOR_CRDG_IN = S.SEND_FOR_CRDG_IN,
                           PS.TERM_RCND_SEND_FOR_CRED_IN = S.TERM_RCND_SEND_FOR_CRED_IN,
                           PS.NETW_SUTE_TOOL_EXT_IN_DT = S.NETW_SUTE_TOOL_EXT_IN_DT,
                           PS.TERM_CTC_EXT_SENT_VNDR_D = S.TERM_CTC_EXT_SENT_VNDR_D,
                           PS.SEND_FOR_CRDG_SET_DT = S.SEND_FOR_CRDG_SET_DT,
                           PS.TERM_RCND_SET_DT = S.TERM_RCND_SET_DT,
                           PS.CRDG_OVRD_IN = S.CRDG_OVRD_IN,
                           PS.CRDG_RQD_CD = S.CRDG_RQD_CD,
                           PS.CRED_CD = S.CRED_CD,
                           PS.INCEXT_SENT_DT = S.INCEXT_SENT_DT,
                           PS.INCEXT_IN = S.INCEXT_IN,
                           PS.PVDR_FLBL_NB = S.PVDR_FLBL_NB,
                           PS.CRDG_CNTC_FRST_NM = S.CRDG_CNTC_FRST_NM,
                           PS.CRDG_CNTCT_LAST_NM = S.CRDG_CNTCT_LAST_NM,
                           PS.EDCN_REQ_SENT_TO_VNDR_DT = S.EDCN_REQ_SENT_TO_VNDR_DT,
                           PS.MDCD_REQ_SENT_TO_VNDR_DT = S.MDCD_REQ_SENT_TO_VNDR_DT,
                           PS.MNL_RSEN_IN = S.MNL_RSEN_IN,
                           PS.MNL_RSEN_DT = S.MNL_RSEN_DT,
                           PS.MNL_UDA_RMVL_IN = S.MNL_UDA_RMVL_IN,
                           PS.MNL_UDA_RMVL_DT = S.MNL_UDA_RMVL_DT,
                           PS.CR2_RMVL_SENT_DT = S.CR2_RMVL_SENT_DT,
                           PS.AUD_LAST_UPDT_TM = S.AUD_LAST_UPDT_TM,
                           PS.SRC_SYS_CD = S.SRC_SYS_CD,
                           PS.LAST_UPDT_SRC_SYS_CD = LAST_UPDT_SRC_SYS_CD,
                           PS.AUD_LOAD_ID = S.AUD_LOAD_ID,
                           PS.AUD_LAST_UPDT_ID = S.AUD_LAST_UPDT_ID,
                           PS.SCRY_ROLE_CD = S.SCRY_ROLE_CD,
                           PS.AUD_CTE_TM = S.AUD_CTE_TM,
                           PS.EVNT_TM = S.EVNT_TM,
                           PS.EVNT_ID = S.EVNT_ID,
                           PS.LAST_UPDT_TM = S.LAST_UPDT_TM

                      WHEN NOT MATCHED THEN INSERT
                          (TNNT_ID,
                           PVDR_ID,
                           CRDG_BGN_DT,
                           CRDG_DLG_IN,
                           CRDG_TYPE_CD,
                           CRDG_END_DT,
                           CRDG_STAT_CD,
                           CRDG_NOTE_TX,
                           CRDG_TERM_RSN_CD,
                           CRDG_CMTE_DT,
                           CRDG_NON_RSPR_IN,
                           CRDG_SENT_TO_VNDR_DT,
                           SEND_FOR_CRDG_IN,
                           TERM_RCND_SEND_FOR_CRED_IN,
                           NETW_SUTE_TOOL_EXT_IN_DT,
                           TERM_CTC_EXT_SENT_VNDR_D,
                           SEND_FOR_CRDG_SET_DT,
                           TERM_RCND_SET_DT,
                           CRDG_OVRD_IN,
                           CRDG_RQD_CD,
                           CRED_CD,
                           INCEXT_SENT_DT,
                           INCEXT_IN,
                           PVDR_FLBL_NB,
                           CRDG_CNTC_FRST_NM,
                           CRDG_CNTCT_LAST_NM,
                           EDCN_REQ_SENT_TO_VNDR_DT,
                           MDCD_REQ_SENT_TO_VNDR_DT,
                           MNL_RSEN_IN,
                           MNL_RSEN_DT,
                           MNL_UDA_RMVL_IN,
                           MNL_UDA_RMVL_DT,
                           CR2_RMVL_SENT_DT,
                           AUD_LAST_UPDT_TM,
                           SRC_SYS_CD,
                           LAST_UPDT_SRC_SYS_CD,
                           AUD_LOAD_ID,
                           AUD_LAST_UPDT_ID,
                           SCRY_ROLE_CD,
                           AUD_CTE_TM,
                           EVNT_TM,
                           EVNT_ID,
                           LAST_UPDT_TM)
                      VALUES
                          (S.TNNT_ID,
                           S.PVDR_ID,
                           S.CRDG_BGN_DT,
                           S.CRDG_DLG_IN,
                           S.CRDG_TYPE_CD,
                           S.CRDG_END_DT,
                           S.CRDG_STAT_CD,
                           S.CRDG_NOTE_TX,
                           S.CRDG_TERM_RSN_CD,
                           S.CRDG_CMTE_DT,
                           S.CRDG_NON_RSPR_IN,
                           S.CRDG_SENT_TO_VNDR_DT,
                           S.SEND_FOR_CRDG_IN,
                           S.TERM_RCND_SEND_FOR_CRED_IN,
                           S.NETW_SUTE_TOOL_EXT_IN_DT,
                           S.TERM_CTC_EXT_SENT_VNDR_D,
                           S.SEND_FOR_CRDG_SET_DT,
                           S.TERM_RCND_SET_DT,
                           S.CRDG_OVRD_IN,
                           S.CRDG_RQD_CD,
                           S.CRED_CD,
                           S.INCEXT_SENT_DT,
                           S.INCEXT_IN,
                           S.PVDR_FLBL_NB,
                           S.CRDG_CNTC_FRST_NM,
                           S.CRDG_CNTCT_LAST_NM,
                           S.EDCN_REQ_SENT_TO_VNDR_DT,
                           S.MDCD_REQ_SENT_TO_VNDR_DT,
                           S.MNL_RSEN_IN,
                           S.MNL_RSEN_DT,
                           S.MNL_UDA_RMVL_IN,
                           S.MNL_UDA_RMVL_DT,
                           S.CR2_RMVL_SENT_DT,
                           S.AUD_LAST_UPDT_TM,
                           S.SRC_SYS_CD,
                           S.LAST_UPDT_SRC_SYS_CD,
                           S.AUD_LOAD_ID,
                           S.AUD_LAST_UPDT_ID,
                           S.SCRY_ROLE_CD,
                           S.AUD_CTE_TM,
                           S.EVNT_TM,
                           S.EVNT_ID,
                           S.LAST_UPDT_TM)"""

    inDf.rdd.coalesce(1).mapPartitions((d) => Iterator(d)).foreach { batch =>
      var dbc: Connection = DriverManager.getConnection(sfConnect,userNm, userPwd)
      var merge: PreparedStatement = dbc.prepareStatement(sqlMerge)
      var i = 0
      try{
        //dbc = DriverManager.getConnection(sfConnect,userNm, userPwd)
        //merge = dbc.prepareStatement(sqlMerge)
        batch.grouped(1000).foreach { session =>
          session.foreach { x => i = 0
            while(i < x.length){
              merge.setObject(i+1, x.get(i))
              i += 1
            }
            merge.addBatch()
          }
          merge.executeBatch()
        }
      }
      catch {
        case ss:SQLException => println("The SQLState received while upsert for tbl PVDR_CRDG_DTL " +ss.getSQLState())
          println("The Message received while upsert for tbl PVDR_CRDG_DTL " + ss.getMessage())
          println("The Error Code received while upsert for tbl PVDR_CRDG_DTL " + ss.getErrorCode())
          println("The Column no - " +i.toString() + " while upsert for tbl PVDR_CRDG_DTL")
          println("get stack trace" +  ss.getStackTraceString)
          println(" SQL Exception found while upsert for tbl PVDR_CRDG_DTL")
          retCode = 0
          var ss1 = ss.getNextException
          while (ss1 != null) {
            println("in add batch entered While loop while upsert for tbl PVDR_CRDG_DTL")
            println("The SQLState received while upsert for tbl PVDR_CRDG_DTL" +ss1.getSQLState())
            println("The Message received while upsert for tbl PVDR_CRDG_DTL " + ss1.getMessage())
            println("The Error Code received while upsert for tbl PVDR_CRDG_DTL " + ss1.getErrorCode())
            ss1 = ss1.getNextException
          }
        case ee1: Exception =>retCode = 0
          println("defined general exception in  inner loop while upsert for tbl EXTL_PVDR ")
          println(ee1.getMessage())
      }
      finally{
        if(dbc != null){
          dbc.commit()
          dbc.close()
        }
      }
    }
    retCode
  }

  def fullDeleteTable(sqlDelete: String, tableNm: String, sfDbDetails: String):Unit={

    val sfConnect=sfDbDetails.split(",")(0)
    val sfSchema=sfDbDetails.split(",")(1)
    val user=sfDbDetails.split(",")(2)
    val userPwd=sfDbDetails.split(",")(3)

    var dbc = DriverManager.getConnection(sfConnect,user,userPwd)
    var delete: PreparedStatement = dbc.prepareStatement(sqlDelete)
    try
    {

      delete.execute()
    }
    catch {
      case ss:SQLException => println("The SQLState received while delete for tbl "  +tableNm+ "- " +ss.getSQLState())
        println("The Message received while delete for tbl "  +tableNm+ "- " + ss.getMessage())
        println("The Error Code received while delete for tbl "  +tableNm+ "- " + ss.getErrorCode())
        println("get stack trace" +  ss.getStackTraceString)
        println(" SQL Exception found")
        var ss1 = ss.getNextException
        while (ss1 != null)
        {
          println("in add batch entered While loop")
          println("The SQLState received while delete for tbl "  +tableNm+ "- " +ss1.getSQLState())
          println("The Message received  while delete for tbl "  +tableNm+ "- " + ss1.getMessage())
          println("The Error Code received  while delete for tbl "  +tableNm+ "- " + ss1.getErrorCode())
          ss1 = ss1.getNextException
        }
      case ee1: Exception =>
        println("Received General exception while delete in table  "  +tableNm)
        println(ee1.getMessage())

    }
    finally
    {
      if(dbc != null)
      {
        dbc.commit()
        dbc.close()
      }
    }
  }
  def main(args:Array[String]): Unit = {

    val startJob = new Timestamp (System.currentTimeMillis())
    val curr_dttm = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSSSS").format(Calendar.getInstance.getTime())
    val curr_date = new SimpleDateFormat("yyyy-MM-dd").format(Calendar.getInstance.getTime())
    //get current date for the logic
    val current_date = java.time.LocalDate.now.toString()
    val dttm = java.time.LocalDate.now.minusDays(180).toString()
    println("Current Date: " + current_date + " Dttm: " + dttm)

    val spark = SparkSession.builder()
      .appName("pvdr_crdg_dtl")
      //.config("spark.master", "local")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    import spark.implicits._

    //SAILFISH DB access credentials
    val stageDbDetails = "jdbc:db2://edcaiasd300:50000/bludb,gdp5s1,svc-hdppvds,ilqdXA89"
    val slfshDbDetails = "jdbc:db2://edcaiasd300:50000/bludb,gdp5u1a,svc-hdppvdu,kZy58LO64acYzqs"
    val sdwDbDetails   = "jdbc:db2://imdsstg01:62033/edw5s1,uat5s1,etldbc,p1ttym3"
    val targetDBDetils = "jdbc:db2://edcaiasd300:50000/bludb,gdp5u1a,svc-hdppvdu,kZy58LO64acYzqs"
    val userInput      = "01/01/1990,yes"
    //val dbDetails = "jdbc:db2://edcaiasd300:50000/bludb,gdp5u1a,J8MX,Ch420840,jdbc:db2://imdsprd01:60354/edw5p1,gda5u1a"
    //sailfish dbDetails

    //sailfish connection details
    val slfshConnect = stageDbDetails.split(",")(0)
    val slfshSchema = stageDbDetails.split(",")(1)
    val user = stageDbDetails.split(",")(2)
    val password = stageDbDetails.split(",")(3)

    //sdw connection details
    val sdwConnect = sdwDbDetails.split(",")(0)
    val sdwSchema = sdwDbDetails.split(",")(1)
    val sdwUser =  sdwDbDetails.split(",")(2)
    val sdwPassword = sdwDbDetails.split(",")(3)

    //user input (ongoing and the current date)
    val prevCutoff = userInput.split(",")(0)
    val ongoing = userInput.split(",")(1)

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


    //-----------------------------------------------------DELETE TABLE---------------------------------------------
    val deleteTableMerge = "DELETE FROM " + "gdp5u1a" + """.PVDR_CRDG_DTL"""
    fullDeleteTable(deleteTableMerge, "PVDR_CRDG_DTL", targetDBDetils)
    //---------------------------------------------------------------------------------------------------------------

    //Provider
    //PVDR Table (DRIVING SOURCE)
    var pvdrStr = s"""(SELECT * FROM ${slfshSchema}.PVDR
                       WHERE ((PVDR_TYPE_NM = 'Organization' AND PVDR_DTL_TYPE_NM IN ('Facility', 'Supplier Business','Group','Mobile'))
                              			OR (PVDR_TYPE_NM = 'Individual' AND PVDR_DTL_TYPE_NM IN ('Professional','Supplier Individual'))))"""
    val pvdrDF = spark.read.jdbc(slfshConnect,pvdrStr,prop)
    println("pvdrDF Count: "+pvdrDF.count())
    //pvdrDF.createOrReplaceTempView("pvdrTmpTbl")

    // PVDR_IRNL_ID_XREF
    var piicrStr = s"""(SELECT PVDR_ID, PVDR_ALTT_ID, ALTT_ID_TYPE_CD FROM ${slfshSchema}.PVDR_IRNL_ID_XREF where ALTT_ID_TYPE_CD = 'PIMS')"""
    val piicrDF = spark.read.jdbc(slfshConnect,piicrStr,prop)

    //PROVIDER_CREDENTIALING_DETAIL
    var pcdStr = s"""(SELECT PCD.PROVIDER_ID as PVDR_ID,
	                           PCD.MANUAL_UDA_REMOVAL_IN as MNL_UDA_RMVL_IN,
	                           PCD.MANUAL_UDA_REMOVAL_DT as MNL_UDA_RMVL_DT,
	                           PCD.CREDENTIALING_TERM_REASON_CD as CRDG_TERM_RSN_CD,
	                           PCD.CREDENTIALING_SENT_TO_VENDOR_DT as CRDG_SENT_TO_VNDR_DT,
	                           PCD.SEND_FOR_CREDENTIALING_IN as SEND_FOR_CRDG_IN,
                             PCD.TERM_RESCIND_SEND_FOR_CRED_IN as TERM_RCND_SEND_FOR_CRED_IN,
                             PCD.NETWORK_SUITE_TOOLS_EXT_IN_DT as NETW_SUTE_TOOL_EXT_IN_DT,
                             PCD.TERMINATED_CTC_EXT_SENT_VNDR_DT as TERM_CTC_EXT_SENT_VNDR_D,
                             PCD.SEND_FOR_CREDENTIALING_SET_DT as SEND_FOR_CRDG_SET_DT,
                             PCD.TERMINATION_RESCIND_SET_DT as TERM_RCND_SET_DT,
                             PCD.CREDENTIALING_OVERRIDE_IN as CRDG_OVRD_IN,
                             PCD.INCEXT_SENT_DT as INCEXT_SENT_DT,
                             PCD.INCEXT_IN as INCEXT_IN,
                             PCD.EDUCATION_REQ_SENT_TO_VNDR_DT as EDCN_REQ_SENT_TO_VNDR_DT,
                             PCD.MEDICAID_REQ_SENT_TO_VNDR_DT as MDCD_REQ_SENT_TO_VNDR_DT,
                             PCD.MANUAL_RESEND_IN as MNL_RSEN_IN,
                             PCD.MANUAL_RESEND_DT as MNL_RSEN_DT,
                             PCD.RECREDENTIALING_REMOVAL_SENT_DT as CR2_RMVL_SENT_DT,
                             PIICR.PROVIDER_ALTERNATE_ID AS PVDR_ALTT_ID
                             FROM ${sdwSchema}.PROVIDER_CREDENTIALING_DETAIL PCD,    ${sdwSchema}.PROVIDER_INTERNAL_IDENTIFIER_CROSS_REFERENCE PIICR
                             WHERE PIICR.PROVIDER_ID = PCD.PROVIDER_ID
                               AND PIICR.ALTERNATE_IDENTIFIER_TYPE_CD = 'PIMS')"""

    var targetPcdStr = s"""(SELECT PCD.PVDR_ID,
                                   PCD.CRDG_TERM_RSN_CD,
                                   PCD.CRDG_SENT_TO_VNDR_DT,
                                   PCD.SEND_FOR_CRDG_IN,
                                   PCD.TERM_RCND_SEND_FOR_CRED_IN,
                                   PCD.NETW_SUTE_TOOL_EXT_IN_DT,
                                   PCD.TERM_CTC_EXT_SENT_VNDR_D,
                                   PCD.SEND_FOR_CRDG_SET_DT,
                                   PCD.TERM_RCND_SET_DT,
                                   PCD.CRDG_OVRD_IN,
                                   PCD.INCEXT_SENT_DT,
                                   PCD.INCEXT_IN,
                                   PCD.EDCN_REQ_SENT_TO_VNDR_DT,
                                   PCD.MDCD_REQ_SENT_TO_VNDR_DT,
                                   PCD.MNL_RSEN_IN,
                                   PCD.MNL_RSEN_DT,
                                   PCD.CR2_RMVL_SENT_DT
                                   FROM ${slfshSchema}.PVDR_CRDG_DTL PCD )"""

    var finalPcdSqlStr:String = ""
    //------------------------------------------INITIAL-------------------------------------------------
    if(ongoing == "yes"){
      println("Initial: ")
      finalPcdSqlStr = pcdStr
      //val pcdDF = spark.read.jdbc(sdwConnect,pcdStr,sdwProp).createOrReplaceTempView("pcd")
      val finalPcdDF = spark.read.jdbc(sdwConnect,finalPcdSqlStr,sdwProp)
      val overarchWdw = Window.partitionBy($"tnnt_id",$"pvdr_id").orderBy($"pvdr_type_nm".desc)

      //SEEDING RULE
      val overarchDF = pvdrDF.as("pvdrDF")
        //.join(piicrDF.as("piicrDF"), $"pvdrDF.PVDR_ID"=== $"piicrDF.PVDR_ALTT_ID", "left_outer")
        //.select($"pvdrDF.*",$"piicrDF.PVDR_ID".as("PVDR_ID_PIICR"),$"piicrDF.PVDR_ALTT_ID".as("PVDR_ALTT_ID"), $"piicrDF.ALTT_ID_TYPE_CD".as("ALTT_ID_TYPE_CD")).as("overarchJoinTmp1")
        .join(finalPcdDF.as("finalPcdDF"), $"finalPcdDF.PVDR_ALTT_ID"===$"pvdrDF.PVDR_ID", "left_outer")
        .select($"pvdrDF.*",$"finalPcdDF.MNL_UDA_RMVL_IN".as("MNL_UDA_RMVL_IN"),
          $"finalPcdDF.MNL_UDA_RMVL_DT".as("MNL_UDA_RMVL_DT"),
          $"finalPcdDF.CRDG_TERM_RSN_CD".as("CRDG_TERM_RSN_CD"),
          $"finalPcdDF.CRDG_SENT_TO_VNDR_DT".as("CRDG_SENT_TO_VNDR_DT"),
          $"finalPcdDF.SEND_FOR_CRDG_IN".as("SEND_FOR_CRDG_IN"),
          $"finalPcdDF.TERM_RCND_SEND_FOR_CRED_IN".as("TERM_RCND_SEND_FOR_CRED_IN"),
          $"finalPcdDF.NETW_SUTE_TOOL_EXT_IN_DT".as("NETW_SUTE_TOOL_EXT_IN_DT"),
          $"finalPcdDF.TERM_CTC_EXT_SENT_VNDR_D".as("TERM_CTC_EXT_SENT_VNDR_D"),
          $"finalPcdDF.SEND_FOR_CRDG_SET_DT".as("SEND_FOR_CRDG_SET_DT"),
          $"finalPcdDF.TERM_RCND_SET_DT".as("TERM_RCND_SET_DT"),
          $"finalPcdDF.CRDG_OVRD_IN".as("CRDG_OVRD_IN"),
          $"finalPcdDF.INCEXT_SENT_DT".as("INCEXT_SENT_DT"),
          $"finalPcdDF.INCEXT_IN".as("INCEXT_IN"),
          $"finalPcdDF.EDCN_REQ_SENT_TO_VNDR_DT".as("EDCN_REQ_SENT_TO_VNDR_DT"),
          $"finalPcdDF.MDCD_REQ_SENT_TO_VNDR_DT".as("MDCD_REQ_SENT_TO_VNDR_DT"),
          $"finalPcdDF.MNL_RSEN_IN".as("MNL_RSEN_IN"),
          $"finalPcdDF.MNL_RSEN_DT".as("MNL_RSEN_DT"),
          $"finalPcdDF.CR2_RMVL_SENT_DT".as("CR2_RMVL_SENT_DT"))
      //.withColumn("rnum",row_number.over(overarchWdw)).filter($"rnum"===1).drop("rnum")

      println("overarchDF Count: " + overarchDF.count())
      overarchDF.printSchema()
      println("Overarch DF: ")
      overarchDF.show(false)
      //Provider Credentialing
      //PVDR_CRDG Table (DRIVING SOURCE)
      val pvdrPcWdw = Window.partitionBy($"pvdr_id",$"tnnt_id").orderBy($"CRDG_BGN_DT".desc)
      var pcStr = s"""(SELECT TNNT_ID, PVDR_ID, CRDG_BGN_DT, CRDG_TYPE_CD, CRDG_END_DT, CRDG_STAT_CD, CRDG_NOTE_TX, CRDG_CMTE_DT, CRDG_NON_RSPR_IN
                       FROM ${slfshSchema}.PVDR_CRDG)"""
      val pcDF = spark.read.jdbc(slfshConnect,pcStr,prop).withColumn("rnum",row_number.over(pvdrPcWdw)).filter($"rnum" === 1)
      println("pcDF Count: "+pcDF.count())
      //pcDF.createOrReplaceTempView("pcTmpTbl")

      //Provider Contract Coverage Line
      //PVDR_CTC_COVA_LN Table (DRIVING SOURCE)
      var pcclStr = s"""(SELECT *
  	                     FROM ${slfshSchema}.PVDR_CTC_COVA_LN
  	                     WHERE COVA_LN_EPRN_DT > '${prevCutoff}')"""
      val pcclDF = spark.read.jdbc(slfshConnect,pcclStr,prop)
      println("pcclDF Count: "+pcclDF.count())
      //pcclDF.createOrReplaceTempView("pcclTmpTbl")

      //Provider Contract Delegation
      //PVDR_CTC_DLGN Table (DRIVING SOURCE)
      var pcdlStr = s"""(SELECT TNNT_ID, PVDR_ID, CTC_ID, CTC_DLGN_TYPE_CD, CTC_DLGN_EPRN_DT
  	                     FROM ${slfshSchema}.PVDR_CTC_DLGN
  	                     WHERE CTC_DLGN_TYPE_CD = 'CR' AND CTC_DLGN_EPRN_DT > '${prevCutoff}')"""
      val pcdlDF = spark.read.jdbc(slfshConnect,pcdlStr,prop)
      println("pcdlDF Count: "+pcdlDF.count())
      //pcdlDF.createOrReplaceTempView("pcdlTmpTb.l")

      //println("PROVIDER_CREDENTIALING_DETAIL Count: " + pcdDF.count())

      //Provider Florida Blue Number
      //PVDR_FLBL_NB (ADDITIONAL SOURCE)
      val pfnWdw = Window.partitionBy($"pvdr_id",$"tnnt_id").orderBy($"FLBL_SAR_DT".asc)
      var pfnSqlStr = s"""(SELECT TNNT_ID, PVDR_ID, FLBL_NB, FLBL_SAR_DT FROM ${slfshSchema}.PVDR_FLBL_NB where FLBL_END_DT > '${prevCutoff}')"""
      val pfnDF = spark.read.jdbc(slfshConnect,pfnSqlStr,prop).withColumn("rnum",row_number.over(pfnWdw)).filter($"rnum" === 1).drop("rnum")

      //Provider_General_Contact Table
      //PVDR_GEN_CNTC (ADDITIONAL SOURCE)
      val pgcWdw = Window.partitionBy($"pvdr_id",$"tnnt_id").orderBy($"CNTC_EFCV_DT".desc)
      var pgcSqlStr = s"""(SELECT TNNT_ID, PVDR_ID, CNTC_TYPE_CD, CNTC_EFCV_DT, CNTC_EPRN_DT, FRST_NM, LAST_NM
  	                              FROM ${slfshSchema}.PVDR_GEN_CNTC
  	                              where CNTC_TYPE_CD = 'CRD'
  	                              AND CNTC_EFCV_DT < '${prevCutoff}'
  	                              AND (CNTC_EPRN_DT > '${prevCutoff}' OR CNTC_EPRN_DT = NULL))"""
      val pgcDF = spark.read.jdbc(slfshConnect,pgcSqlStr,prop).withColumn("rnum",row_number.over(pgcWdw)).filter($"rnum" === 1).drop("rnum")

      //Provider_Contract
      //PCTC (ADDITIONAL SOURCE)
      var pctcSqlStr = s"""(SELECT * FROM gdp5s1.PVDR_CTC WHERE CTC_EPRN_DT > '${prevCutoff}')"""
      val pctcDF = spark.read.jdbc(slfshConnect,pctcSqlStr,prop)
      /*
      //CRDG_DLG_IN
      //Credentialing Delegated Indicator (not table)
      val cdiWdw = Window.partitionBy($"pvdr_id",$"ctc_id").orderBy($"COVA_LN_EPRN_DT".desc)
      val CRDG_DLG_IN_Join = pcclDF.as("pcclDF")
                           .join(pcdlDF.as("pcdlDF"), Seq("TNNT_ID","PVDR_ID","CTC_ID"), "inner")
                           .select ($"pcclDF.PVDR_ID".as("PVDR_ID"), $"pcclDF.CTC_ID".as("CTC_ID")).as("tempSemiJoin1")
                           .join(pctcDF.as("pctcDF"), Seq("CTC_ID"), "left_outer")
                           .select($"tempSemiJoin1.PVDR_ID")
                           .withColumn("rnum",row_number.over(cdiWdw)).filter($"rnum"===1).drop("rnum")
      */
      //final join

      //val crdgRqdCdDF = functionToCalculate_CRDG_RQD_CD(spark,pvdrDF,pcclDF,pctcDF,slfshSchema,slfshConnect,prop,prevCutoff)

      val join1DF = overarchDF.as("overarchDF")
        .join(pcDF.as("pcDF"), Seq ("TNNT_ID","PVDR_ID"), "left_outer")
        .select($"overarchDF.*", $"pcDF.CRDG_BGN_DT".as("CRDG_BGN_DT"), $"pcDF.CRDG_TYPE_CD".as("CRDG_TYPE_CD"),
          $"pcDF.CRDG_END_DT".as("CRDG_END_DT"), $"pcDF.CRDG_STAT_CD".as("CRDG_STAT_CD"), $"pcDF.CRDG_NOTE_TX".as("CRDG_NOTE_TX"),
          $"pcDF.CRDG_CMTE_DT".as("CRDG_CMTE_DT"), $"pcDF.CRDG_NON_RSPR_IN".as("CRDG_NON_RSPR_IN"))
        //case statements when no match
        .withColumn("CRDG_BGN_DT",when($"CRDG_BGN_DT".isNull,lit("1900-01-01")).otherwise(col("CRDG_BGN_DT")))
        .withColumn("CRDG_TYPE_CD",when($"CRDG_TYPE_CD".isNull,lit(" ")).otherwise(col("CRDG_TYPE_CD")))
        .withColumn("CRDG_END_DT",when($"CRDG_END_DT".isNull,lit("9999-12-31")).otherwise(col("CRDG_END_DT"))).as("tempJoin1")
      println("Join 1 Count: " + join1DF.count())
      val join2DF = join1DF.as("tempJoin1").join(pfnDF.as("pfnDF"), Seq ("PVDR_ID"), "left_outer")
        .select($"tempJoin1.*", $"pfnDF.FLBL_NB".as("FLBL_NB"), $"pfnDF.FLBL_SAR_DT".as("FLBL_SAR_DT")).as("tempJoin2")
      println("Join 2 Count: " + join2DF.count())
      val join3DF = join2DF.as("tempJoin2").join(pgcDF.as("PGCDF"), Seq("PVDR_ID"), "left_outer")
        .select($"tempJoin2.*", $"pgcDF.FRST_NM".as("FRST_NM"), $"pgcDF.LAST_NM".as("LAST_NM"))
        .withColumn("AUD_LAST_UPDT_TM",lit(curr_dttm))
        .withColumn("AUD_CTE_TM",lit(curr_dttm))
        .withColumn("LAST_UPDT_TM",lit(curr_dttm))
        .withColumn("MNL_RSEN_DT",when(col("MNL_RSEN_DT").isNull,lit(curr_date)).otherwise(col("MNL_RSEN_DT")))
        .withColumn("MNL_UDA_RMVL_DT",when(col("MNL_UDA_RMVL_DT").isNull,lit(curr_date)).otherwise(col("MNL_UDA_RMVL_DT")))
        .withColumn("CR2_RMVL_SENT_DT",when(col("CR2_RMVL_SENT_DT").isNull,lit(curr_date)).otherwise(col("CR2_RMVL_SENT_DT")))
        .withColumn("CRDG_STAT_CD",when(col("CRDG_STAT_CD").isNull,lit(" ")).otherwise(col("CRDG_STAT_CD")))
        .withColumn("CRDG_NON_RSPR_IN",when(col("CRDG_NON_RSPR_IN").isNull,lit(0)).otherwise(col("CRDG_NON_RSPR_IN")))
        .withColumn("FLBL_NB",when(col("FLBL_NB").isNull,lit("ERROR")).otherwise(col("FLBL_NB")))
        .withColumn("TERM_RCND_SEND_FOR_CRED_IN",when(col("TERM_RCND_SEND_FOR_CRED_IN").isNull,lit(0)).otherwise(col("TERM_RCND_SEND_FOR_CRED_IN")))
        .withColumn("CRDG_OVRD_IN",when(col("CRDG_OVRD_IN").isNull,lit(0)).otherwise(col("CRDG_OVRD_IN")))
        .withColumn("CRDG_TERM_RSN_CD",when(col("CRDG_TERM_RSN_CD").isNull,lit("ERROR")).otherwise(col("CRDG_TERM_RSN_CD")))
        .withColumn("CRDG_SENT_TO_VNDR_DT",when(col("CRDG_SENT_TO_VNDR_DT").isNull,lit(curr_date)).otherwise(col("CRDG_SENT_TO_VNDR_DT")))
        .withColumn("SEND_FOR_CRDG_IN",when(col("SEND_FOR_CRDG_IN").isNull,lit(0)).otherwise(col("SEND_FOR_CRDG_IN")))

      println("Join 3 Count: " + join3DF.count())

      join1DF.printSchema()
      join3DF.createOrReplaceTempView("join3DF")
      val finalDF = spark.sql("""SELECT 1 AS TNNT_ID,
                                        PVDR_ID,
                                        CRDG_BGN_DT,
                                        1 AS CRDG_DLG_IN,
                                        CRDG_TYPE_CD,
                                        CRDG_END_DT,
                                        CRDG_STAT_CD,
                                        CRDG_NOTE_TX,
                                        CRDG_TERM_RSN_CD,
                                        CRDG_CMTE_DT,
                                        CRDG_NON_RSPR_IN,
                                        CRDG_SENT_TO_VNDR_DT,
                                        SEND_FOR_CRDG_IN,
                                        TERM_RCND_SEND_FOR_CRED_IN,
                                        NETW_SUTE_TOOL_EXT_IN_DT,
                                        TERM_CTC_EXT_SENT_VNDR_D,
                                        SEND_FOR_CRDG_SET_DT,
                                        TERM_RCND_SET_DT,
                                        CRDG_OVRD_IN,
                                        'no' AS CRDG_RQD_CD,
                                        'no' AS CRED_CD,
                                        INCEXT_SENT_DT,
                                        INCEXT_IN,
                                        FLBL_NB AS PVDR_FLBL_NB,
                                        FRST_NM AS CRDG_CNTC_FRST_NM,
                                        LAST_NM AS CRDG_CNTCT_LAST_NM,
                                        EDCN_REQ_SENT_TO_VNDR_DT,
                                        MDCD_REQ_SENT_TO_VNDR_DT,
                                        MNL_RSEN_IN,
                                        MNL_RSEN_DT,
                                        MNL_UDA_RMVL_IN,
                                        MNL_UDA_RMVL_DT,
                                        CR2_RMVL_SENT_DT,
                                        AUD_LAST_UPDT_TM,
                                        '363' AS SRC_SYS_CD,
                                        '363' AS LAST_UPDT_SRC_SYS_CD,
                                        'pvdr_crdg_dtl' AS AUD_LOAD_ID,
                                        'pvdr_crdg_dtl' AS AUD_LAST_UPDT_ID,
                                        'GEN' AS SCRY_ROLE_CD,
                                        AUD_CTE_TM,
                                        NULL AS EVNT_TM,
                                        NULL AS EVNT_ID,
                                        LAST_UPDT_TM
                                        from join3DF""")
      finalDF.show()

      var errDF = finalDF.filter(col("CRDG_BGN_DT").isNull )
      println("CRDG_BGN_DT  - "+errDF.count())

      errDF = finalDF.filter(col("TNNT_ID").isNull )
      println("TNNT_ID  - "+errDF.count())

      errDF = finalDF.filter(col("PVDR_ID").isNull )
      println("PVDR_ID  - "+errDF.count())

      errDF = finalDF.filter(col("CRDG_DLG_IN").isNull )
      println("CRDG_DLG_IN  - "+errDF.count())

      errDF = finalDF.filter(col("CRDG_TYPE_CD").isNull )
      println("CRDG_TYPE_CD  - "+errDF.count())

      errDF = finalDF.filter(col("CRDG_STAT_CD").isNull )
      println("CRDG_STAT_CD  - "+errDF.count())

      errDF = finalDF.filter(col("CRDG_TERM_RSN_CD").isNull )
      println("CRDG_TERM_RSN_CD  - "+errDF.count())

      errDF = finalDF.filter(col("CRDG_NON_RSPR_IN").isNull )
      println("CRDG_NON_RSPR_IN  - "+errDF.count())

      errDF = finalDF.filter(col("SEND_FOR_CRDG_IN").isNull )
      println("SEND_FOR_CRDG_IN  - "+errDF.count())

      errDF = finalDF.filter(col("TERM_RCND_SEND_FOR_CRED_IN").isNull )
      println("TERM_RCND_SEND_FOR_CRED_IN  - "+errDF.count())

      errDF = finalDF.filter(col("CRDG_OVRD_IN").isNull )
      println("CRDG_OVRD_IN  - "+errDF.count())

      errDF = finalDF.filter(col("CRDG_RQD_CD").isNull )
      println("CRDG_RQD_CD  - "+errDF.count())

      errDF = finalDF.filter(col("CRED_CD").isNull )
      println("CRED_CD  - "+errDF.count())

      errDF = finalDF.filter(col("PVDR_FLBL_NB").isNull )
      println("PVDR_FLBL_NB  - "+errDF.count())

      errDF = finalDF.filter(col("AUD_LAST_UPDT_TM").isNull )
      println("AUD_LAST_UPDT_TM  - "+errDF.count())

      errDF = finalDF.filter(col("SRC_SYS_CD").isNull )
      println("SRC_SYS_CD  - "+errDF.count())

      errDF = finalDF.filter(col("LAST_UPDT_SRC_SYS_CD").isNull )
      println("LAST_UPDT_SRC_SYS_CD  - "+errDF.count())

      errDF = finalDF.filter(col("AUD_LOAD_ID").isNull )
      println("AUD_LOAD_ID  - "+errDF.count())

      errDF = finalDF.filter(col("AUD_LAST_UPDT_ID").isNull )
      println("AUD_LAST_UPDT_ID  - "+errDF.count())

      errDF = finalDF.filter(col("LAST_UPDT_TM").isNull )
      println("LAST_UPDT_TM  - "+errDF.count())


      //errDF.show(false)
      //println("errDF.count() : "+errDF.count())

      //val finalFinalDF = finalDF.where("CR2_RMVL_SENT_DT is not null and MNL_UDA_RMVL_DT is not null and MNL_RSEN_DT is not null")
      println("Am i going crazy: " + finalDF.count())
      writeToDatabase(finalDF,targetDBDetils)
      println("Writing might be done, check logs")
    }

    //--------------------------------------ONGOING-----------------------------------------
    else {
      println("Ongoing: ")
      finalPcdSqlStr = targetPcdStr
      //val pcdDF = spark.read.jdbc(sdwConnect,pcdStr,sdwProp).createOrReplaceTempView("pcd")
      val targetPcdDF = spark.read.jdbc(slfshConnect,finalPcdSqlStr,prop)

      //SEEDING RULE
      val overarchDF = pvdrDF.as("pvdrDF")
        //.join(piicrDF.as("piicrDF"), Seq("PVDR_ID"), "left_outer")  	                         .select($"pvdrDF.*",$"piicrDF.PVDR_ALTT_ID".as("PVDR_ALTT_ID"), $"piicrDF.ALTT_ID_TYPE_CD".as("ALTT_ID_TYPE_CD")).as("overarchJoinTmp1")
        .join(targetPcdDF.as("targetPcdDF"), Seq("PVDR_ID"), "left_outer")
        .select($"pvdrDF.*",$"targetPcdDF.*")
        .createOrReplaceTempView("targetPCD")

      val sqlTemp="""CASE WHEN CRDG_RQD_CD = 'No' THEN 'Not Required'
CASE WHEN CRDG_DLG_IN = '1' THEN 'Yes'
CASE WHEN CRDG_OVRD_IN = '1'
CASE WHEN CRDG_TYPE_CD in ('02','03') and CRDG_STAT_CD = 'AP'
THEN 'Yes' ELSE 'No' END CRED_CD"""




      // add s in front of string if it does'nt work
      val finalPcdDF = spark.sql("""SELECT TNNT_ID,
  	                                       PVDR_ID,
  	                                       CRDG_TERM_RSN_CD,
  	                                       CASE WHEN MNL_RSEN_DT IS NULL THEN CRDG_SENT_TO_VNDR_DT
                                           WHEN MNL_RSEN_DT > CRDG_SENT_TO_VNDR_DT
                                           THEN MNL_RSEN_DT
                                           WHEN CRDG_SENT_TO_VNDR_DT IS NULL
                                           THEN MNL_RSEN_DT
                                           ELSE CRDG_SENT_TO_VNDR_DT END AS CRDG_SENT_TO_VNDR_DT,
	                                         SEND_FOR_CRDG_IN,
	                                         TERM_RCND_SEND_FOR_CRED_IN,
	                                         NETW_SUTE_TOOL_EXT_IN_DT,
	                                         CASE WHEN (MNL_RSEN_DT IS NULL) THEN TERM_CTC_EXT_SENT_VNDR_D
	                                              WHEN (MNL_RSEN_DT > TERM_CTC_EXT_SENT_VNDR_D) THEN NULL
	                                              ELSE TERM_CTC_EXT_SENT_VNDR_D END AS TERM_CTC_EXT_SENT_VNDR_D,
	                                         SEND_FOR_CRDG_SET_DT,
	                                         TERM_RCND_SET_DT,
	                                         CASE WHEN (CRED_MNL_CD = 'OVERCRED') THEN 1
	                                              ELSE 0 END AS CRDG_OVRD_IN,
	                                         CASE WHEN (CRED_MNL_CD = 'INCEXT' or CRED_MNL_CD = 'SPECPROJ') THEN '${prevCutoff}'
	                                              ELSE INCEXT_SENT_DT END AS INCEXT_SENT_DT,
	                                         CASE WHEN (CRED_MNL_CD = 'INCEXT') THEN 1
	                                              WHEN (CRED_MNL_CD = 'SPECPROJ') THEN 2
	                                              ELSE INCEXT_IN END AS INCEXT_IN,
	                                         EDCN_REQ_SENT_TO_VNDR_DT,
	                                         MDCD_REQ_SENT_TO_VNDR_DT,
	                                         CASE WHEN (CRED_MNL_CD = 'NRSEND') THEN 1
	                                              ELSE MNL_RSEN_IN END AS MNL_RSEN_IN,
	                                         CASE WHEN (  CRED_MNL_CD = 'NRSEND' AND MNL_RSEN_DT IS NULL) THEN '${prevCutoff}'
	                                              ELSE MNL_RSEN_DT END AS MNL_RSEN_DT,
	                                         CR2_RMVL_SENT_DT	  ,
                                             CASE WHEN CRED_MNL_CD = 'REMEXT' THEN 1
                                             ELSE CRED_MNL_CD END as CRED_MNL_CD,
                                             CASE WHEN  MNL_UDA_RMVL_DT is null then '${prevCutoff}'
                                             ELSE MNL_UDA_RMVL_DT END AS MNL_UDA_RMVL_DT
  	                                       FROM targetPCD""")

      //SEEDING RULE

      //Provider Credentialing
      //PVDR_CRDG Table (DRIVING SOURCE)
      val wdwPvdr = Window.partitionBy($"tnnt_id",$"pvdr_id").orderBy($"CRDG_BGN_DT".desc)
      var pcStr = s"""(SELECT TNNT_ID, PVDR_ID, CRDG_BGN_DT, CRDG_TYPE_CD, CRDG_END_DT, CRDG_STAT_CD, CRDG_NOTE_TX, CRDG_CMTE_DT, CRDG_NON_RSPR_IN
                       FROM ${slfshSchema}.PVDR_CRDG)"""
      val pcDF = spark.read.jdbc(slfshConnect,pcStr,prop).withColumn("rnum",row_number.over(wdwPvdr)).filter($"rnum" === 1)
      println("pcDF Count: "+pcDF.count())
      //pcDF.createOrReplaceTempView("pcTmpTbl")

      //Provider Contract Coverage Line
      //PVDR_CTC_COVA_LN Table (DRIVING SOURCE)
      var pcclStr = s"""(SELECT TNNT_ID, PVDR_ID, CTC_ID, COVA_LN_EPRN_DT
  	                     FROM ${slfshSchema}.PVDR_CTC_COVA_LN
  	                     WHERE COVA_LN_EPRN_DT > '${prevCutoff}')"""
      val pcclDF = spark.read.jdbc(slfshConnect,pcclStr,prop)
      println("pcclDF Count: "+pcclDF.count())
      //pcclDF.createOrReplaceTempView("pcclTmpTbl")

      //Provider Contract Delegation
      //PVDR_CTC_DLGN Table (DRIVING SOURCE)
      var pcdlStr = s"""(SELECT TNNT_ID, PVDR_ID, CTC_ID, CTC_DLGN_TYPE_CD, CTC_DLGN_EPRN_DT
  	                     FROM ${slfshSchema}.PVDR_CTC_DLGN
  	                     WHERE CTC_DLGN_TYPE_CD = 'CR' AND CTC_DLGN_EPRN_DT > '${prevCutoff}')"""
      val pcdlDF = spark.read.jdbc(slfshConnect,pcdlStr,prop)
      println("pcdlDF Count: "+pcdlDF.count())
      //pcdlDF.createOrReplaceTempView("pcdlTmpTb.l")

      //println("PROVIDER_CREDENTIALING_DETAIL Count: " + pcdDF.count())

      //Provider Florida Blue Number
      //PVDR_FLBL_NB (ADDITIONAL SOURCE)
      val wdwPFN = Window.partitionBy($"pvdr_id").orderBy($"FLBL_SAR_DT".asc)
      var pfnSqlStr = s"""(SELECT PVDR_ID, FLBL_NB, FLBL_SAR_DT FROM ${slfshSchema}.PVDR_FLBL_NB)"""
      val pfnDF = spark.read.jdbc(slfshConnect,pfnSqlStr,prop).withColumn("rnum",row_number.over(wdwPFN)).filter($"rnum" === 1).drop("rnum")


      //Provider_General_Contact Table
      //PVDR_GEN_CNTC (ADDITIONAL SOURCE)

      val pgcWdw = Window.partitionBy($"pvdr_id",$"tnnt_id").orderBy($"CNTC_EFCV_DT".desc)

      var pgcSqlStr = s"""(SELECT PVDR_ID, CNTC_TYPE_CD, CNTC_EFCV_DT, CNTC_EPRN_DT, FRST_NM, LAST_NM
  	                              FROM ${slfshSchema}.PVDR_GEN_CNTC
  	                              where CNTC_TYPE_CD = 'CRD'
  	                              AND CNTC_EFCV_DT < '${prevCutoff}'
  	                              AND (CNTC_EPRN_DT > '${prevCutoff}' OR CNTC_EPRN_DT = NULL))"""
      val pgcDF = spark.read.jdbc(slfshConnect,pgcSqlStr,prop).withColumn("rnum",row_number.over(pgcWdw)).filter($"rnum" === 1).drop("rnum")

      //Provider_Contract
      //PCTC (ADDITIONAL SOURCE)
      var pctcSqlStr = s"""(SELECT *
  	                       FROM ${slfshSchema}.PVDR_CTC
  	                       WHERE CTC_EPRN_DT > '${prevCutoff}')"""
      val pctcDF = spark.read.jdbc(slfshConnect,pctcSqlStr,prop)

      //CRDG_DLG_IN
      //Credentialing Delegated Indicator (not table)
      val cdiWdw = Window.partitionBy($"pvdr_id",$"ctc_id").orderBy($"COVA_LN_EPRN_DT".desc)
      val CRDG_DLG_IN_Join = pcclDF.as("pcclDF")
        .join(pcdlDF.as("pcdlDF"), Seq("TNNT_ID","PVDR_ID","CTC_ID"), "left_outer")
        .select ($"pcclDF.PVDR_ID".as("PVDR_ID"), $"pcclDF.CTC_ID".as("CTC_ID")).as("tempSemiJoin1")
        .join(pctcDF.as("pctcDF"), Seq("CTC_ID"), "left_outer")
        .select($"tempSemiJoin1.PVDR_ID").withColumn("rnum",row_number.over(cdiWdw)).filter($"rnum"===1).drop("rnum")

      val joinFinalDF = finalPcdDF.as("finalPcdDF")
        .join(pcDF.as("pcDF"), Seq ("TNNT_ID","PVDR_ID"), "left_outer")
        .select($"finalPcdDF.*", $"pcDF.CRDG_BGN_DT".as("CRDG_BGN_DT"), $"pcDF.CRDG_TYPE_CD".as("CRDG_TYPE_CD"),
          $"pcDF.CRDG_END_DT".as("CRDG_END_DT"), $"pcDF.CRDG_STAT_CD".as("CRDG_STAT_CD"), $"pcDF.CRDG_NOTE_TX".as("CRDG_NOTE_TX"),
          $"pcDF.CRDG_CMTE_DT".as("CRDG_CMTE_DT"), $"pcDF.CRDG_NON_RSPR_IN".as("CRDG_NON_RSPR_IN")).withColumn("CRDG_BGN_DT",when($"CRDG_BGN_DT".isNull,lit("1900-01-01")).otherwise(col("CRDG_BGN_DT")))
        .withColumn("CRDG_TYPE_CD",when($"CRDG_TYPE_CD".isNull,lit(" ")).otherwise(col("CRDG_TYPE_CD")))
        .withColumn("CRDG_END_DT",when($"CRDG_END_DT".isNull,lit("9999-12-31")).otherwise(col("CRDG_END_DT"))).as("tempJoin1")
        .join(pfnDF.as("pfnDF"), Seq ("PVDR_ID"), "left_outer")
        .select($"tempJoin1.*", $"pfnDF.FLBL_NB".as("FLBL_NB"),
          $"pfnDF.FLBL_SAR_DT".as("FLBL_SAR_DT")).as("tempJoin2")
        .join(pgcDF.as("PGCDF"), Seq("PVDR_ID"), "left_outer")
        .select($"tempJoin2.*", $"pgcDF.FRST_NM".as("FRST_NM"), $"pgcDF.LAST_NM".as("LAST_NM"))
        .withColumn("AUD_LAST_UPDT_TM",lit(curr_dttm))
        .withColumn("AUD_CTE_TM",lit(curr_dttm))
        .withColumn("LAST_UPDT_TM",lit(curr_dttm))
        .withColumn("MNL_RSEN_DT",when(col("MNL_RSEN_DT").isNull,lit(curr_date)).otherwise(col("MNL_RSEN_DT")))
        .withColumn("MNL_UDA_RMVL_DT",when(col("MNL_UDA_RMVL_DT").isNull,lit(curr_date)).otherwise(col("MNL_UDA_RMVL_DT")))
        .withColumn("CR2_RMVL_SENT_DT",when(col("CR2_RMVL_SENT_DT").isNull,lit(curr_date)).otherwise(col("CR2_RMVL_SENT_DT")))
        .withColumn("CRDG_STAT_CD",when(col("CRDG_STAT_CD").isNull,lit(" ")).otherwise(col("CRDG_STAT_CD")))
        .withColumn("CRDG_NON_RSPR_IN",when(col("CRDG_NON_RSPR_IN").isNull,lit(0)).otherwise(col("CRDG_NON_RSPR_IN")))
        .withColumn("FLBL_NB",when(col("FLBL_NB").isNull,lit("ERROR")).otherwise(col("FLBL_NB")))
        .withColumn("TERM_RCND_SEND_FOR_CRED_IN",when(col("TERM_RCND_SEND_FOR_CRED_IN").isNull,lit(0)).otherwise(col("TERM_RCND_SEND_FOR_CRED_IN")))
        .withColumn("CRDG_OVRD_IN",when(col("CRDG_OVRD_IN").isNull,lit(0)).otherwise(col("CRDG_OVRD_IN")))
        .withColumn("CRDG_TERM_RSN_CD",when(col("CRDG_TERM_RSN_CD").isNull,lit("ERROR")).otherwise(col("CRDG_TERM_RSN_CD")))
        .withColumn("CRDG_SENT_TO_VNDR_DT",when(col("CRDG_SENT_TO_VNDR_DT").isNull,lit(curr_date)).otherwise(col("CRDG_SENT_TO_VNDR_DT")))
        .withColumn("SEND_FOR_CRDG_IN",when(col("SEND_FOR_CRDG_IN").isNull,lit(0)).otherwise(col("SEND_FOR_CRDG_IN")))


      println("joinFinalDF Count: " + joinFinalDF.count())
      joinFinalDF.createOrReplaceTempView("joinFinalDF")
      val finalDF = spark.sql("""SELECT 1 AS TNNT_ID,
                                        PVDR_ID,
                                        CRDG_BGN_DT,
                                        1 AS CRDG_DLG_IN,
                                        CRDG_TYPE_CD,
                                        CRDG_END_DT,
                                        CRDG_STAT_CD,
                                        CRDG_NOTE_TX,
                                        CRDG_TERM_RSN_CD,
                                        CRDG_CMTE_DT,
                                        CRDG_NON_RSPR_IN,
                                        CRDG_SENT_TO_VNDR_DT,
                                        SEND_FOR_CRDG_IN,
                                        TERM_RCND_SEND_FOR_CRED_IN,
                                        NETW_SUTE_TOOL_EXT_IN_DT,
                                        TERM_CTC_EXT_SENT_VNDR_D,
                                        SEND_FOR_CRDG_SET_DT,
                                        TERM_RCND_SET_DT,
                                        CRDG_OVRD_IN,
                                        'no' AS CRDG_RQD_CD,
                                        'no' AS CRED_CD,
                                        INCEXT_SENT_DT,
                                        INCEXT_IN,
                                        FLBL_NB AS PVDR_FLBL_NB,
                                        FRST_NM AS CRDG_CNTC_FRST_NM,
                                        LAST_NM AS CRDG_CNTCT_LAST_NM,
                                        EDCN_REQ_SENT_TO_VNDR_DT,
                                        MDCD_REQ_SENT_TO_VNDR_DT,
                                        MNL_RSEN_IN,
                                        MNL_RSEN_DT,
                                        MNL_UDA_RMVL_IN,
                                        MNL_UDA_RMVL_DT,
                                        CR2_RMVL_SENT_DT,
                                        AUD_LAST_UPDT_TM,
                                        '363' AS SRC_SYS_CD,
                                        '363' AS LAST_UPDT_SRC_SYS_CD,
                                        'pvdr_crdg_dtl' AS AUD_LOAD_ID,
                                        'pvdr_crdg_dtl' AS AUD_LAST_UPDT_ID,
                                        'GEN' AS SCRY_ROLE_CD,
                                        AUD_CTE_TM,
                                        NULL AS EVNT_TM,
                                        NULL AS EVNT_ID,
                                        LAST_UPDT_TM
                                        from joinFinalDF""")
      finalDF.show()
      var errDF = finalDF.filter(col("CRDG_BGN_DT").isNull )
      println("CRDG_BGN_DT  - "+errDF.count())

      errDF = finalDF.filter(col("TNNT_ID").isNull )
      println("TNNT_ID  - "+errDF.count())

      errDF = finalDF.filter(col("PVDR_ID").isNull )
      println("PVDR_ID  - "+errDF.count())

      errDF = finalDF.filter(col("CRDG_DLG_IN").isNull )
      println("CRDG_DLG_IN  - "+errDF.count())

      errDF = finalDF.filter(col("CRDG_TYPE_CD").isNull )
      println("CRDG_TYPE_CD  - "+errDF.count())

      errDF = finalDF.filter(col("CRDG_STAT_CD").isNull )
      println("CRDG_STAT_CD  - "+errDF.count())

      errDF = finalDF.filter(col("CRDG_TERM_RSN_CD").isNull )
      println("CRDG_TERM_RSN_CD  - "+errDF.count())

      errDF = finalDF.filter(col("CRDG_NON_RSPR_IN").isNull )
      println("CRDG_NON_RSPR_IN  - "+errDF.count())

      errDF = finalDF.filter(col("SEND_FOR_CRDG_IN").isNull )
      println("SEND_FOR_CRDG_IN  - "+errDF.count())

      errDF = finalDF.filter(col("TERM_RCND_SEND_FOR_CRED_IN").isNull )
      println("TERM_RCND_SEND_FOR_CRED_IN  - "+errDF.count())

      errDF = finalDF.filter(col("CRDG_OVRD_IN").isNull )
      println("CRDG_OVRD_IN  - "+errDF.count())

      errDF = finalDF.filter(col("CRDG_RQD_CD").isNull )
      println("CRDG_RQD_CD  - "+errDF.count())

      errDF = finalDF.filter(col("CRED_CD").isNull )
      println("CRED_CD  - "+errDF.count())

      errDF = finalDF.filter(col("PVDR_FLBL_NB").isNull )
      println("PVDR_FLBL_NB  - "+errDF.count())

      errDF = finalDF.filter(col("AUD_LAST_UPDT_TM").isNull )
      println("AUD_LAST_UPDT_TM  - "+errDF.count())

      errDF = finalDF.filter(col("SRC_SYS_CD").isNull )
      println("SRC_SYS_CD  - "+errDF.count())

      errDF = finalDF.filter(col("LAST_UPDT_SRC_SYS_CD").isNull )
      println("LAST_UPDT_SRC_SYS_CD  - "+errDF.count())

      errDF = finalDF.filter(col("AUD_LOAD_ID").isNull )
      println("AUD_LOAD_ID  - "+errDF.count())

      errDF = finalDF.filter(col("AUD_LAST_UPDT_ID").isNull )
      println("AUD_LAST_UPDT_ID  - "+errDF.count())

      errDF = finalDF.filter(col("LAST_UPDT_TM").isNull )
      println("LAST_UPDT_TM  - "+errDF.count())

    println("Am i going crazy: " + finalDF.count())
    writeToDatabase(finalDF,targetDBDetils)
    println("Writing might be done, check logs")
    }
    //wdwPvdr = Window.partitionBy($"pvdr_id",$"tnnt_id").orderBy($"CRDG_BGN_DT".desc)

    //.withColumn("rnum",row_number.over(wdwPvdr)).filter($"rnum" === 1)

    /*
    valjoin1 = pvdrDF.join(pcDF.as("pcDF"), Seq("PVDR_ID"), "left_outer")
    val wdwFlblnb = Window.partitionBy($"pvdr_id").orderBy($"pvdr_id".desc)
    //(1)where wdwFlblnb IS NULL then default values


    val df1 = join1.withColumn("rnum",row_number.over(wdwFlblnb))

    --three sets i.e.
              (2) max(rnum) >1 then dedup
              (3) max(rnum) =1 then straight move

    --Union above three dataset

    df1.withColumn("rnum", max("purchase_date") over Window.partitionBy($"pvdr_id").orderBy($"pvdr_id".desc))
        .filter($"purchase_date" === $"most_recent_purchase_in_group")
   */

  }
}
