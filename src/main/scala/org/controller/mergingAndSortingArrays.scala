package org.controller

object mergingAndSortingArrays {
  def mergeArrays(a: Array[Int], b: Array[Int]): Array[Int] = {
    val concatenatedArray=a ++ b
    val concatenatedList=concatenatedArray.toList
    val concatenatedListTemp=concatenatedList.sortWith(_<_) // sorting in ascnding
    concatenatedListTemp.toArray
  }

  def main(args: Array[String]): Unit = {
    val a=Array(10,34,56,78)
    val b=Array(11,22,9,98)
    val c =mergeArrays(a,b)
    for (i <- c)
      println(i)
  }
}
