package org.controller
import scala.util.control.Breaks._
object breakExample {
  def main(args: Array[String]): Unit = {
    for (i <- 0 to 100)
      {
        breakable{// it act's as continue. if you need to break the loop keep the for loop inside the  breakable block
          if (i%10==0)
            break;
          println("i= "+i)
        }
      }
  }

}
