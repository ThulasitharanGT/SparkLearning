package org.controller.AdvancedTopic

object MatchCaseEG {
  def main(args: Array[String]): Unit = {
    val a:Any=5.0

    a match {
      case  i:Int => println("Int")
      case  d:Double  => println("Double")
      case  _ => println("Other")
    }
  }

}
