package org.controller.explorations.tmpExploration

object implementTrait extends traitTmp with traitTwo { //we can do multiple inheritance in trait
  override def cool: Unit = {
    println("Cool")
  }
  override val t ="tmp"
    override def good(): Unit = {
      println("Good")
    }
}

/*Scala provides @transient annotation for fields. If the field is marked as @transient, then the framework should not save the field even when the surrounding object is serialized. When the object is loaded, the field will be restored to the default value for the type of the field annotated as @transient.*/
