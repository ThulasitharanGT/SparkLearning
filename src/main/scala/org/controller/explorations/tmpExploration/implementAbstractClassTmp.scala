package org.controller.explorations.tmpExploration

object implementAbstractClassTmp extends abstractClassTmp/* with abstractClassTwo */{
  override def cool: Unit = {
    println("Cool")
  }
/*  override def good(): Unit = { , we cant do multiple inheritance in abstract class
    println("Good")
  }*/
}
