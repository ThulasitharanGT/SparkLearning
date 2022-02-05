package org.controller

class auxillaryConstructor {
  var a:Int=0
  var b:Int=0
  def this(a:Int,b:Int)={
    this()
    this.a=a
    this.b=b
  }
  def this(a:Int)={
    this()
    this.a=a
    this.b=1
  }
}
