package org.controller.AdvancedTopic

object HigherOrderExample {

  def main(args: Array[String]): Unit = {

    println(HighFun(x=>{x*x},1,5))
    println(HighFun(x=>{x*x},5,1))
    println(HighFunAdvanced(x=>{x*x},1,5))

  }
  def HighFun(f:(Int)=>Int,a:Int,b:Int):Int=
  {
    var Result=0
    for (i <- a to b)
      Result=Result+f(i)
    Result
  }

  def HighFunAdvanced(f:(Int)=>Int,a:Int,b:Int):Int=
  {
    var Result=0
    a match {
      case value if value > b =>  for (i <- b to a) Result = Result + f(i)
      case value if value < b =>  for (i <- a to b) Result = Result + f(i)
      case value if value == b =>  Result = Result + f(value)
      case _ => println("Inalid SCN")
    }
    Result
  }

}
