package controller

object TrimbleProblem {

  def main(args: Array[String]): Unit = {

    val a = 1 to 10
    var k =collection.mutable.Map[Int,Int] ()
    val n = 10 to 20
    var KCounter: Int = 0
    for (i <- n) {
      print(i + " ")
    }
    for (x <- n) {
      var checkerPrimeCount: Int = 0
      val Factors = checker(x)
      for (i <- 0 to (Factors.size)-1 ) {
        if (prime(Factors(i)) == 2) {
          //println("Factors.size" + Factors.size)
          checkerPrimeCount = checkerPrimeCount + 1
          //println("checkerPrimeCount " + checkerPrimeCount)

        }
      }
      if (checkerPrimeCount == Factors.size) {
        k.put(KCounter,x)
        KCounter = KCounter + 1
      }
    }
    for (i <- 0 to (k.size) -1) {
      print(k(i) + " ")
    }

  }


  def prime(value: Int): Int = {
    var Count: Int = 0
    for (temp <- 1 to value) {
      if (value % temp == 0) {
        Count = Count + 1
      }
    }
    Count
  }


  def checker(value: Int) = {
    val Factors=collection.mutable.Map[Int,Int] ()
    var Count: Int = 0
    for (temp <- 1 to value) {
      if (value % temp == 0) {
        Factors.put(Count,temp)
        Count = Count + 1
      }
    }
    Factors
  }

}