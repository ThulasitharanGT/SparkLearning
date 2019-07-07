package controller
import util.SparkOpener
import org.apache.spark.sql.functions._

object SplittingOneColumnToMultiple extends SparkOpener {

  def main(args: Array[String]): Unit = {
    val spark = SparkSessionLoc("SparkSession")
    val survey_df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("D:\\study\\survey.csv") //.na.fill(" ")
    survey_df.createOrReplaceTempView("survey_view")

   // spark.sql("select instr(no_employees,'-')    from survey_view where id = 1").show

    var temp_df=  spark.sql(""" select * from (select case when lower(trim(gender)) in (cast('make' as String) , cast('cis man' as String) , cast('m' as String) ,cast('male' as String)) then 'Male'
 |      when lower(trim(gender)) in ( cast('cis female' as String) ,cast('female' as String) , cast('women' as String) , cast('f' as String) , cast('w' as String)) then  'Female'
 |      else  'Others' end as Updated_Gender,
        case when lower(trim(treatment)) ='yes' then 1 when lower(trim(treatment)) = 'no' then 0 end as Result
        , case when instr(no_employees,'-') = 3 then no_employees when instr(no_employees,'-') = 4 then  concat(concat(substring(no_employees,5,6),'-'),substring(no_employees,1,3))
               else 'NA' end as no_employees_updated
 |         from survey_view) a
                where no_employees_updated  not in ('NA') """.stripMargin)
   // temp_df.filter("no_employees_updated  not in ('NA')")show()

    val temp_df_no_employees_updated_splitted=split(temp_df("no_employees_updated"),"-")
    temp_df= temp_df.withColumn("Day",when(temp_df_no_employees_updated_splitted.getItem(0) > 31,31).otherwise(temp_df_no_employees_updated_splitted.getItem(0)))
    temp_df= temp_df.withColumn("Month",when(temp_df_no_employees_updated_splitted.getItem(1) >1 ,"Dec".toString()).otherwise(temp_df_no_employees_updated_splitted.getItem(1)) )
    temp_df.show()
  }

}
