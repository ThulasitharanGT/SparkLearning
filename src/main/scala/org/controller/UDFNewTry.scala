package org.controller
import org.util.SparkOpener

object UDFNewTry extends SparkOpener {
  val spark = SparkSessionLoc("SparkSession")
  def main(args: Array[String]): Unit =
  {
    val survey_df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(System.getProperty("user.dir")+"\\Input\\survey.csv") //.na.fill(" ")
    survey_df.createOrReplaceTempView("survey_view")
    survey_df.printSchema
    spark.sql(""" select case when lower(trim(gender)) in (cast('make' as String) , cast('cis man' as String) , cast('m' as String) ,cast('male' as String)) then 'Male'
 |      when lower(trim(gender)) in ( cast('cis female' as String) ,cast('female' as String) , cast('women' as String) , cast('f' as String) , cast('w' as String)) then  'Female'
 |      else  'Others' end as Updated_Gender,
                case when lower(trim(treatment)) ='yes' then 1 when lower(trim(treatment)) = 'no' then 0 end as Result
                , case when lower(trim(tech_company)) ='yes'  then 'Tech' when  lower(trim(coworkers)) ='yes' then 'Coworker' else work_interfere end as temp_new from survey_view
                 """.stripMargin).createOrReplaceTempView("Temp_survey_new")
    spark.sql("select * from Temp_survey_new").show

    spark.sql(
      """ select  a.Updated_Gender as gender ,all_yes,all_no from
        |(select Updated_Gender,count(Result) as all_no from Temp_survey_new where Result=0 group by Updated_Gender) a  join
        |(select Updated_Gender,count(Result) as all_yes from Temp_survey_new where Result=1 group by Updated_Gender)b on a.Updated_Gender=b.Updated_Gender""".stripMargin).show
  }

}
