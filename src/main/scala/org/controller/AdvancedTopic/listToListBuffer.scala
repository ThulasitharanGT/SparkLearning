package org.controller.AdvancedTopic
import scala.collection.mutable.ListBuffer
object listToListBuffer {
  val inputList = List(1,2,3,6,7,8,12,13,14,16,19,20,98,99,54,99,100)
  val lb=new ListBuffer[List[Int]]()
  var tL=new Array[Int](inputList.size)
  var tLCounter=0

def addElementToArray(currentIndex:Int)
={
  tL(tLCounter)=inputList(currentIndex)
  tLCounter=tLCounter+1
}
  def addElementToListBuffer()={
    lb+=tL.slice(0, tLCounter).toList // slice gets a sub array of array
    tL=new Array[Int](inputList.size)
    tLCounter=0
  }
 def checkingElementsOtherThanLastElement(currentIndex:Int)={
    if ((inputList(currentIndex)+1) == inputList(currentIndex+1))
      addElementToArray(currentIndex)
    else
    if (tL.size >0)
      {
        addElementToArray(currentIndex)
        addElementToListBuffer()
      }
    else
       addElementToArray(currentIndex)
 }
  def checkingLastElement(currentIndex:Int)={
    if (inputList(currentIndex-1)+1 == inputList(currentIndex))
      addElementToArray(currentIndex)
    else
    if (tL.size >0)
    {
      addElementToArray(currentIndex)
      addElementToListBuffer()
    }
    else
    {
      addElementToArray(currentIndex)
      addElementToListBuffer()
    }
  }

  def main(args:Array[String]):Unit= {

    for (i <- 0 to inputList.size-1) // checking first element to last element-1
      {
        if(i < inputList.size-1)
        checkingElementsOtherThanLastElement(i)
        else    //  if(i == inputList.size-1) // for checking last element alone . if or else is same due to for loop
        checkingLastElement(i)
      }

      // println("i= "+i)
      // println("a.size = "+a.size)
    //  println(a(i))
    // if  (i<a.size-1) {println(a(i+1))} else {println("last element")}

      /*{
        checkingElementsOtherThanLastElement(i)
       // println("equals First")
        if((inputList(i)+1) == inputList(i+1))
        {
         // println("equals Second")
          addElementToArray(i)
        //  println("equals Second tlCounter = "+tLCounter)

        }
        else
        {
          if (tL.size >0)
          {
          //  println("appending last element to existing array ")
            addElementToArray(i)
            addElementToListBuffer()
         //   println("appending last element to existing array tlCounter = "+tLCounter)
          }
          else
          {
           // println("nw array")
            addElementToArray(i)
         //   println("nw array tlCounter = "+tLCounter)
         //   lb+=tL.slice(0, tLCounter).toList
         //   tL=new Array[Int](inputList.size)
          }
        }
      }*/



       /*{
        //println("last size element")
        if (inputList(i-1)+1 == inputList(i))
        {
         // println("lats size element inside")
          addElementToArray(i)
          addElementToListBuffer()
        }
        else
          {
            if (tL.size >0)
            {
             // println("appending last element to existing array ") already would have spitted previous element in before iteration
              addElementToArray(i)
              addElementToListBuffer()
           //   println("appending last element to existing array tlCounter = "+tLCounter)
            }
            else
            {
            //  println("nw array")
              addElementToArray(i)
            //  println("nw array tlCounter = "+tLCounter)
              addElementToListBuffer()
            }
          }

      }*/

    for (t <- lb)
        println(t)
  }

}
