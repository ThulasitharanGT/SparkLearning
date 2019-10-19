package org.controller
import org.util.{SparkOpener, readWriteUtil}
import org.constants.projectConstants
import org.apache.spark.sql.functions._

object UDFNewTry extends SparkOpener {
  val spark = SparkSessionLoc("SparkSession")

  def main(args: Array[String]): Unit =
  {
    val inputMap=collection.mutable.Map[String,String]()
    inputMap.put(projectConstants.fileFormatArg,projectConstants.csvFormat)
    inputMap.put(projectConstants.fileTypeArgConstant,projectConstants.fileTypeCsvValue)
    inputMap.put(projectConstants.delimiterArgConstant,projectConstants.delimiterComma)
    inputMap.put(projectConstants.headerArgConstant,projectConstants.stringTrue)
    inputMap.put(projectConstants.inferSchemaArgConstant,projectConstants.stringTrue)
    inputMap.put(projectConstants.basePathValueConstant,System.getProperty("user.dir")+"\\Input")
    inputMap.put(projectConstants.filePathArgValue,System.getProperty("user.dir")+"\\Input\\survey.csv")
    val survey_df =readWriteUtil.readDF(spark,inputMap) //spark.read.format("csv").option("header", "true").option("inferSchema", "true").option("delimiter","|").load(System.getProperty("user.dir")+"\\Input\\survey.csv") //.na.fill(" ")
    //SqlVersion
    survey_df.createOrReplaceTempView("survey_view")
    survey_df.printSchema
    val sqlString=""" select case when lower(trim(gender)) in (cast('make' as String) , cast('cis man' as String) , cast('cis man' as String) ,cast('male' as String)) then 'Male'
      when lower(trim(gender)) in ( cast('cis female' as String) ,cast('female' as String) , cast('women' as String) , cast('f' as String) , cast('w' as String)) then  'Female'
      else  'Others' end as Updated_Gender,
                case when lower(trim(treatment)) ='yes' then 1 when lower(trim(treatment)) = 'no' then 0 end as Result
                , case when lower(trim(tech_company)) ='yes'  then 'Tech' when  lower(trim(coworkers)) ='yes' then 'Coworker' else work_interfere end as temp_new from survey_view
                 """
    readWriteUtil.execSparkSql(spark,sqlString.stripMargin).createOrReplaceTempView("Temp_survey_new")
    readWriteUtil.execSparkSql(spark,"select * from Temp_survey_new").show

    readWriteUtil.execSparkSql(spark,
      """ select  a.Updated_Gender as gender ,all_yes,all_no from
        |(select Updated_Gender,count(Result) as all_no from Temp_survey_new where Result=0 group by Updated_Gender) a  join
        |(select Updated_Gender,count(Result) as all_yes from Temp_survey_new where Result=1 group by Updated_Gender)b on a.Updated_Gender=b.Updated_Gender""".stripMargin).show
    // Udf Version
    spark.udf.register("genderRefiner",genderAssigner(_:String))
    spark.udf.register("treatmentRefiner",treatmentAssigner(_:String))
    spark.udf.register("workRefiner",workAssigner(_:String,_:String,_:String))

    val surveyTemp=survey_df.selectExpr("genderRefiner(cast(gender as String)) as Updated_Gender","treatmentRefiner(cast(treatment as String)) as Treatment_Result","workRefiner(cast(tech_company as String),cast(coworkers as String),cast(work_interfere as String)) as Work_Result")
    surveyTemp.show
    val noDF= surveyTemp.filter("Treatment_Result=0").groupBy("Updated_Gender","Work_Result").agg(count("Treatment_Result").as("all_No"))
    val yesDF=surveyTemp.filter("Treatment_Result=1").groupBy("Updated_Gender","Work_Result").agg(count("Treatment_Result").as("all_Yes"))
    val finalDf=noDF.join(yesDF,Seq("Updated_Gender","Work_Result")).orderBy(desc("Updated_Gender"),desc("Work_Result"))
    finalDf.show

  }
  def genderAssigner(stringVal:String):String ={
    stringVal.toLowerCase match {
      case value if value == "cis female" || value=="female" || value=="women" || value=="f" || value=="w" => "Female"
      case value if value == "make" || value=="cis man" || value=="m" || value=="male" || value=="man" => "Male"
      case _ => "Others"
    }
  }
  def treatmentAssigner (stringVal:String) ={
    stringVal.trim.toLowerCase match {
      case value if value == "yes"  => 1
      case value if value== "no"  => 0
      case _ => -1
    }
  }
  def workAssigner (techCompany:String,coworkers:String,work_interfere:String) ={
    techCompany.trim.toLowerCase match {
      case value if value == "yes"  => "Tech"
      case value if value== "no"  => coworkers.trim.toLowerCase match {case value if value == "yes" => "Co-Worker"; case _ =>work_interfere}
      case _ => work_interfere
    }
  }
}
