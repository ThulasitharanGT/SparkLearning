package org.controller.AdvancedTopic

object TailRecursion {

  def main(args: Array[String]): Unit = {
    println(tfact(5,1))
  }
def tfact(x:Int,CurrentAns:Int):Int={
  if(x<=0)
    CurrentAns
  else
    tfact(x-1,CurrentAns*x)
}
}
