package org.controller.explorations

import java.text.SimpleDateFormat
//import java.util.Date

import org.util.SparkOpener
import scala.util.control.Breaks._
import scala.util.Try
import sys.process._

object houseLoanPredictor extends SparkOpener{

  val spark=SparkSessionLoc("cool")
  import spark.implicits._
  spark.sparkContext.setLogLevel("ERROR")
def main(args:Array[String]):Unit={
  val dfBuffer=collection.mutable.ArrayBuffer[expenseClass] ()
  val dateFormat= new SimpleDateFormat("yyyy-MM")

/*  val years=1 to 22
  val months = 1 to 12
  val currentYear=2021 // planned from next year so increment works here
  val salaryMonthlyConstant=60000
  val loanDueMonthlyConstant=30000
  val expenditureMonthlyConstant=30000
  val totalLoanToBePaidConstant=7733695*/
  var amountPaid=0
  val inputMap=collection.mutable.Map[String,String]()
  for (arg <- args)
    {
      val keyPart = arg.split("=",2)(0)
      val valPart = arg.split("=",2)(1)
      println(s"key=${keyPart} value=${valPart}")
      inputMap.put(keyPart,valPart)
    }

  val numYears=Try{inputMap("numYears")}.isSuccess match {case true => inputMap("numYears").toInt case false => 20}

 // val currentYear=2021 // planned from next year so increment works here
  val startingYear=inputMap("startingYear").toInt
  val salaryMonthlyConstant=inputMap("salaryMonthly").toInt
  val loanDueMonthlyConstant=inputMap("loanDueMonthly").toInt
  val expenditureMonthlyConstant=inputMap("expenditureMonthly").toInt
  val totalLoanToBePaidConstant=inputMap("totalLoanToBePaid").toInt
  val outputPathHdfs=inputMap("outputPathHdfs")
  val outputPathLocal=inputMap("outputPathLocal")
  val startMonth= Try{inputMap("startMonth")}.isSuccess match {case true => inputMap("startMonth").toInt case false => 1}
  val salaryIncrementPercentage=inputMap("salaryIncrementPercentage").toInt
  val expenseIncrementPercentage=inputMap("expenseIncrementPercentage").toInt

  val months = startMonth match { case value if value ==1 => 1 to 12 case _ => monthSeqGenerator(startMonth)}
  val numYearsFinal= startMonth match {case value if value ==1  => numYears case _ => numYears+1}
  val years= 1 to numYearsFinal

  println(s"Input Info ............................")
  println(s"numYears = ${numYears}")
  println(s"startingYear = ${startingYear}")
  println(s"salaryMonthlyConstant = ${salaryMonthlyConstant}")
  println(s"loanDueMonthlyConstant = ${loanDueMonthlyConstant}")
  println(s"expenditureMonthlyConstant = ${expenditureMonthlyConstant}")
  println(s"totalLoanToBePaidConstant = ${totalLoanToBePaidConstant}")
  println(s"outputPathHdfs = ${outputPathHdfs}")
  println(s"outputPathLocal = ${outputPathLocal}")
  println(s"startMonth = ${startMonth}")
  println(s"salaryIncrementPercentage = ${salaryIncrementPercentage}")
  println(s"expenseIncrementPercentage = ${expenseIncrementPercentage}")
  println(s"months = ${months}")
  println(s"years = ${years}")

  breakable
  {
    for (year <- years) //val year=1
      for (month <- months)//val month=1
      {
        totalLoanToBePaidConstant-amountPaid match {
          case value if value >0 =>
            val currentYear=startingYear + (year match {case 1 => 0 case _ => year-1})
            println(s"currentYear - ${currentYear}")
            //  val startDate=dateFormat.parse(s"${currentYear+year}-${month}") // planned from 22 to 21 + year's will go through
            val startDate=dateFormat.parse(s"${currentYear}-${month}")
          //  val salaryExpected=year match {case 1 => salaryMonthly case _ =>{ for (numIncrement <- 1 to year -1) salaryMonthly=salaryMonthly+((salaryMonthly/100)*2) ;salaryMonthly.toInt}
            val salaryExpected=year match {case 1 => salaryMonthlyConstant case _ => incrementer(salaryMonthlyConstant,year,salaryIncrementPercentage) }
         //   val expenseExpected=year match {case 1 => expenditureMonthly case _ => {for (numIncrement <- 1 to year -1) expenditureMonthly=expenditureMonthly+((expenditureMonthly/100)*1) ; expenditureMonthly.toInt}}
            val expenseExpected=year match {case 1 => expenditureMonthlyConstant case _ => incrementer(expenditureMonthlyConstant,year,expenseIncrementPercentage)}
            val amountPaidTmp=amountPaid+loanDueMonthlyConstant
            val remainingLoanToBePaid=totalLoanToBePaidConstant-amountPaidTmp match { case value if value > 0 => value case _ => 0} //add the remaining to extra sal
            // val amountPaidTmp= 4
            println(s"${dateFormat.format(startDate)}")
            println(s"salaryExpected - ${salaryExpected}")
            println(s"expenseExpected - ${expenseExpected}")
            println(s"amountPaidTmp - ${amountPaidTmp}")
            println(s"remainingLoanToBePaid - ${remainingLoanToBePaid}")
            val amountPaidFinal=totalLoanToBePaidConstant-amountPaidTmp match { // -5 match {
              case value if value > 0  =>
                amountPaid=amountPaidTmp
                loanDueMonthlyConstant
              case _ =>                              // check here
                amountPaid=amountPaid+(totalLoanToBePaidConstant-amountPaid)
              loanDueMonthlyConstant- Math.abs(totalLoanToBePaidConstant-amountPaidTmp)//  math.abs(amountPaidTmp-totalLoanToBePaidConstant)  // both are of same logic
            }
            println(s"amountPaidFinal - ${amountPaidFinal}")
            val remainingExpected=salaryExpected-expenseExpected -( loanDueMonthlyConstant -amountPaidFinal match {case 0 => loanDueMonthlyConstant case _ => amountPaidFinal})
            println(s"remainingExpected - ${remainingExpected}")
            dfBuffer+=expenseClass(s"${dateFormat.format(startDate)}",salaryExpected,amountPaidFinal,expenseExpected,remainingExpected,remainingLoanToBePaid,amountPaid)
            // expenditure increases 1% by year
            // salary increases 2% by year

          case _ =>
            println(s"Loan completed in - ${dateFormat.format(dateFormat.parse(s"${startingYear+ (year match {case 1 => 0 case _ => year-1})}-${month-1}"))}")
            break
            println("break executed")  // check here
        }
      }
  }
  //dfBuffer.clear
  dfBuffer.toSeq.toDF.orderBy("monthYear").show(dfBuffer.size,false)
  dfBuffer.toSeq.toDF.repartition(2).coalesce(1).orderBy("monthYear").write.mode("append").format("csv").option("header","true").option("delimiter","|").save(outputPathHdfs)//.save("hdfs://localhost:8020/user/raptor/temp/")
  val outputFiles=s"hdfs dfs -ls ${outputPathHdfs} "#|"sort -k7,8"!!;
  val outputFilesList=outputFiles.split("\n").slice(1,outputFiles.split("\n").size).filter(! _.contains("_SUCCESS")).map(x => x.substring( x.contains("hdfs:") match {case true => x.indexOf("/")-5 case false => x.indexOf("/")}))
  val outputFileName=outputFilesList(outputFilesList.size -1)
  println(s"outputFileName - ${outputFileName}")
  println(s"Get command - hdfs dfs -get ${outputFileName} ${outputPathLocal}")

  s"hdfs dfs -get ${outputFileName} ${outputPathLocal}"!;
  // s"hdfs dfs -rm -r hdfs://localhost:8020/user/raptor/temp/part* "!
}
  def monthSeqGenerator(startMonth:Int)={
    val monthArrayBuffer= collection.mutable.ArrayBuffer[Int]()
    var monthNumber=startMonth
    var monthAddCounter=0
    while (monthAddCounter <12)
      monthNumber match {
      case value if value >12 => monthNumber=monthNumber-12 ;  monthArrayBuffer+=monthNumber ; monthNumber=monthNumber+1; monthAddCounter=monthAddCounter+1
      case _ =>   monthArrayBuffer+=monthNumber ; monthNumber=monthNumber+1; monthAddCounter=monthAddCounter+1
      }
    monthArrayBuffer.toSeq
  }

  def incrementer(baseNum:Int,numTimes:Int,percentage:Int)={
    var outputNum=baseNum
    for (numIncrement <- 1 to numTimes -1)
      outputNum=outputNum+((outputNum/100)*percentage)
    outputNum
  }
}
