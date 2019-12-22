package org.controller.AdvancedTopic
import scala.collection.mutable.ListBuffer
object listToListBuffer {

  def main(args:Array[String]):Unit= {
    val a = List(1,2,3,6,7,8,12,13,14,16,19,20,98,99,54,99,100)
    val lb=new ListBuffer[List[Int]]()


    var tL=new Array[Int](a.size)
    var tLCounter=0
    for (i <- 0 to a.size-1)
    {
      // println("i= "+i)
      // println("a.size = "+a.size)
    //  println(a(i))
    // if  (i<a.size-1) {println(a(i+1))} else {println("last elemnt")}
       if(i < a.size-1)
      {
       // println("equals First")
        if((a(i)+1) == a(i+1))
        {
         // println("equals Second")
          tL(tLCounter)=a(i)
          tLCounter=tLCounter+1
        //  println("equals Second tlCounter = "+tLCounter)

        }
        else
        {
          if (tL.size >0)
          {
          //  println("appending last element to existing array ")
            tL(tLCounter)=a(i)
            tLCounter=tLCounter+1
            lb+=tL.slice(0, tLCounter).toList // slice gets a sub array of array
            tL=new Array[Int](a.size)
            tLCounter=0
         //   println("appending last element to existing array tlCounter = "+tLCounter)
          }
          else
          {
           // println("nw array")
            tL(tLCounter)=a(i)
            tLCounter=tLCounter+1
         //   println("nw array tlCounter = "+tLCounter)
            lb+=tL.slice(0, tLCounter).toList
            tL=new Array[Int](a.size)
          }
        }
      }
      if(i == a.size-1)
      {
        //println("last size element")
        if (a(i-1)+1 == a(i))
        {
         // println("lats size element inside")
          tL(tLCounter)=a(i)
          tLCounter=tLCounter+1
          lb+=tL.slice(0, tLCounter).toList
        }
        else
          {
            if (tL.size >0)
            {
             // println("appending last element to existing array ")
              tL(tLCounter)=a(i)
              tLCounter=tLCounter+1
              lb+=tL.slice(0, tLCounter).toList
              tL=new Array[Int](a.size)
              tLCounter=0
           //   println("appending last element to existing array tlCounter = "+tLCounter)
            }
            else
            {
            //  println("nw array")
              tL(tLCounter)=a(i)
              tLCounter=tLCounter+1
            //  println("nw array tlCounter = "+tLCounter)
              lb+=tL.slice(0, tLCounter).toList
              tL=new Array[Int](a.size)
            }
          }

      }
    }
    for (t <- lb)
      {
        println(t)
      }
  }

}
