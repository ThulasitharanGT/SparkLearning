package org.controller.AdvancedTopic

object HigherOrderFunExample {

  def main(args: Array[String]): Unit = {
    println(fungetter(23,(y:Int)=>{"["+y+"]"}))
  }

 // def fungetter(a:Int,funcr:(Int)=>String) = funcr(a)
 def fungetter(a:Int,f :(Int)=>String) = f (a)
}
