package org.controller
import org.apache.spark.sql.{Encoder, Encoders}

import scala.reflect.runtime.universe.TypeTag

class UDAFextention[I <: lapData : TypeTag,P <: lapData: TypeTag,O <: lapData: TypeTag] extends org.apache.spark.sql.expressions.Aggregator[I,P,O]  {

  override def zero: P = ???

  override def reduce(b: P, a: I): P = b

  override def merge(b1: P, b2: P): P = b2

  override def finish(reduction: P): O = ???

  override def bufferEncoder: Encoder[P] =  org.apache.spark.sql.Encoders.product[P]

  override def outputEncoder: Encoder[O] = org.apache.spark.sql.Encoders.product[O]
}
