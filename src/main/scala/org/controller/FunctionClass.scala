package org.controller

import org.constants.projectConstants
import org.controller.UDFNewTry.spark
import org.util.{SparkOpener, readWriteUtil}

object FunctionClass extends SparkOpener
{
  val spark=SparkSessionLoc("SparkSession")
  def main(args: Array[String]): Unit = {

    val inputMap=collection.mutable.Map[String,String]()
    inputMap.put(projectConstants.fileFormatArg,projectConstants.csvFormat)
    inputMap.put(projectConstants.fileTypeArgConstant,projectConstants.fileTypeCsvValue)
    inputMap.put(projectConstants.delimiterArgConstant,projectConstants.delimiterComma)
    inputMap.put(projectConstants.headerArgConstant,projectConstants.stringTrue)
    inputMap.put(projectConstants.inferSchemaArgConstant,projectConstants.stringTrue)
    inputMap.put(projectConstants.basePathValueConstant,System.getProperty("user.dir")+"\\Input")
    inputMap.put(projectConstants.filePathArgValue,System.getProperty("user.dir")+"\\Input\\survey.csv")
    val survey_df =readWriteUtil.readDF(spark,inputMap)
    survey_df.show(10,projectConstants.booleanFalse)
    survey_df.createOrReplaceTempView("survey_view")
   val survey_correct= readWriteUtil.execSparkSql(spark,
     """select case when lower(trim(gender)) in ('make' , 'cis man' , 'm','male') then 'Male'
         when lower(trim(gender)) in ('cis female','female','women','f','w')
       then 'Female' else 'Others' end as Gender_Refined ,* from survey_view""".stripMargin)
    survey_correct.show()
    survey_correct.createOrReplaceTempView("survey_view_ref")
    val result= readWriteUtil.execSparkSql(spark,
      """select A.Gender_Refined,tot_Yes,tot_No from
        ((select Gender_Refined,count(treatment) as tot_Yes from survey_view_ref where treatment='Yes' group by Gender_Refined)a
        full Join
        (select Gender_Refined as Gender_Temp ,count(treatment) as tot_No from survey_view_ref where treatment='No' group by Gender_Refined )b
        on a.Gender_Refined=b.Gender_Temp) a """.stripMargin)
    result.show()

    val Temp_Result_Yes=readWriteUtil.execSparkSql(spark,"select Gender_Refined,count(treatment) as tot_Yes from survey_view_ref where treatment='Yes' group by Gender_Refined")
    val Temp_Result_No=readWriteUtil.execSparkSql(spark,"select Gender_Refined,count(treatment) as tot_No from survey_view_ref where treatment='No' group by Gender_Refined")
    val Temp_Result=Temp_Result_Yes.join(Temp_Result_No,"Gender_Refined")
    Temp_Result.show()
  }
  //spark.close()

}

