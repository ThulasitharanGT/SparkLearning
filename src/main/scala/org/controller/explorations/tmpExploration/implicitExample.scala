package org.controller.explorations.tmpExploration

object implicitExample {
def main(args:Array[String]):Unit ={
  val tmpVal="tmp"
  implicit val tmpImpVal="tmpImpVal" // if you don't pass a value to implicit variable in function. It will automatically take the implicit value which is present for its data type in current bock and pass it
//  implicit val tmpImpVal2="tmpImpVal" // Only one implicit value per DType must be specified in that main block
  println(s"result ${checkMethod(tmpVal)}")
  println(s"result ${checkMethod}")
  if (true){
  //  implicit val tmpImpVal2="tmpImpVal" // Only one implicit value per DType must be specified in that main block
    println(s"result ${checkMethod}")
  }

}
  def checkMethod(implicit implicitVal:String)= s"now - ${implicitVal}"
}
