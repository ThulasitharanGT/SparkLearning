package org.controller

import org.util.SparkOpener

object FunctionClass extends SparkOpener
{
  val spark=SparkSessionLoc("SparkSession")
  def main(args: Array[String]): Unit = {
    val survey_df=spark.read.format("csv").option("header","true").option("inferSchema","true").load(System.getProperty("user.dir")+"\\Input\\survey.csv")//.na.fill(" ")
    survey_df.show(10,false)
    survey_df.createOrReplaceTempView("survey_view")
   val survey_correct= spark.sql(
     """select case when lower(trim(gender)) in ('make' , 'cis man' , 'm','male') then 'Male'
         when lower(trim(gender)) in ('cis female','female','women','f','w')
       then 'Female' else 'Others' end as Gender_Refined ,* from survey_view""".stripMargin)
    survey_correct.show()
    survey_correct.createOrReplaceTempView("survey_view_ref")
    val result= spark.sql(
      """select A.Gender_Refined,tot_Yes,tot_No from
        ((select Gender_Refined,count(treatment) as tot_Yes from survey_view_ref where treatment='Yes' group by Gender_Refined)a
        full Join
        (select Gender_Refined as Gender_Temp ,count(treatment) as tot_No from survey_view_ref where treatment='No' group by Gender_Refined )b
        on a.Gender_Refined=b.Gender_Temp) a """.stripMargin)
    result.show()

    val Temp_Result_Yes=spark.sql("select Gender_Refined,count(treatment) as tot_Yes from survey_view_ref where treatment='Yes' group by Gender_Refined")
    val Temp_Result_No=spark.sql("select Gender_Refined,count(treatment) as tot_No from survey_view_ref where treatment='No' group by Gender_Refined")
    val Temp_Result=Temp_Result_Yes.join(Temp_Result_No,"Gender_Refined")
    Temp_Result.show()
  }
  //spark.close()

}

