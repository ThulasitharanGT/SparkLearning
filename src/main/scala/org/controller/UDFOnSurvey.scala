package org.controller
import org.util.SparkOpener

 object UDFOnSurvey extends SparkOpener {
  val spark = SparkSessionLoc("SparkSession")

  def main(args: Array[String]): Unit = {

    val survey_df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(System.getProperty("user.dir")+"\\Input\\survey.csv") //.na.fill(" ")
    survey_df.show(10, false)
    survey_df.createOrReplaceTempView("survey_view")
    val pgenderFun=pgender(_:String)
    spark.udf.register("val_gender",pgenderFun)
    val Yes_Fun=Yes_No(_:String,"yes")
    val No_Fun=Yes_No(_:String,"no")
    val YesFun=Yes_Fun(_:String)
    val NoFun=No_Fun(_:String)
    spark.udf.register("YesFun",YesFun)
    spark.udf.register("NoFun",NoFun)
    spark.sql(""" select val_gender(gender) as Updated_Gender,
                 YesFun(treatment) as Yes_Result , NoFun(treatment) as No_Result
                from survey_view
                 """.stripMargin).createOrReplaceTempView("Temp_survey")//show(10,false

    val Yes_DF=spark.sql(
      """select Updated_Gender , count(Yes_Result) as Tot_Yes_Result from Temp_survey  where Yes_Result=1 group by Updated_Gender""".stripMargin)
    val No_DF=spark.sql(
      """select Updated_Gender , count(No_Result) as Tot_No_Result from Temp_survey  where No_Result=1 group by Updated_Gender""".stripMargin)
    val Final_DF=Yes_DF.join(No_DF,"Updated_Gender")//.show()
    Final_DF.show()
    // Final_DF.write.mode("overwrite").saveAsTable("Final_Table")

  }

  def pgender(SValue: String) = {
    SValue.trim.toLowerCase match {
      case "make" | "cis man" | "m" | "male" => "Male"
      case "cis female" | "female" | "women" | "f" | "w" => "Female"
      case _ => "Others"
    }
  }

  def Yes_No (SValue: String,Yes_No_String : String) = {
    SValue.trim.toLowerCase match {
      case Yes_No_String => 1
      case _ => 0
    }

  }
}



