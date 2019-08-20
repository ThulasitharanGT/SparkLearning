package org.controller.AdvancedTopic

object FunctionTry {

  def main(args: Array[String]): Unit = {
    val a=RetFun(5)
    println(a(4))
    val customers=Array("Mike","Michael","Lando","Carlos")

    // using map function to map another function to elements of a collection
       customers.map(println)

    // multiple line
    for (i <- 0 to customers.length-1 )
      {
        println(customers(i) +" , welcome !")
      }
    // defining a partially ordered function
    val paymentReminder =Reminder("payment reminder for ",_:String)

    // all operators are method's in scala, so methods are made to work like operator's hence all method's can follow this

    customers foreach paymentReminder

    customers.map(paymentReminder)

  }
def RetFun(x:Int)=(i:Int)=>{

  if ( x%2 == 0) {i*2}
  else if (x%2 != 0 ) {i*3}
  else  {i}
}
//partially ordered function

  def  Reminder(x:String,y:String)={println( x+y)}


}
