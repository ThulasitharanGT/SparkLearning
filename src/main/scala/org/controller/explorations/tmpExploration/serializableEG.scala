package org.controller.explorations.tmpExploration

class serializableEG extends Serializable with traitTwo {
 @transient val tmp= "this wont be serialized" // this wont be serialized
 val tmp2="this will be serialized"
  @transient override val t= ??? //this wont be serialized
@transient def ct()={} //this wont be serialized

  /*
  Scala provides @transient annotation for fields.
  If the field is marked as @transient, then the framework should not save the field even when the surrounding object is serialized.
  When the object is loaded, the field will be restored to the default value for the type of the field annotated as @transient.
  */
  override def good(): Unit = ??? //this wont be serialized
}
