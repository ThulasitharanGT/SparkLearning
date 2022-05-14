package org.controller.markCalculation
import org.apache.spark.sql.Row
import org.apache.spark.sql.streaming.GroupState
// import scala.jdk.CollectionConverters._ for scala  2.13
import scala.collection.JavaConverters._

import java.util

class flatMapGroupFunction(inputMap:collection.mutable.Map[String,String]) extends  org.apache.spark.api.java.function.FlatMapGroupsWithStateFunction[(String,String),org.apache.spark.sql.Row,dynamicSchemaSCD2.stateStore,org.apache.spark.sql.Row] {
  override def call(k: (String, String), iterator: util.Iterator[Row], groupState: GroupState[dynamicSchemaSCD2.stateStore]): util.Iterator[Row] = dynamicSchemaSCD2.stateFunction(k,iterator.asScala.toList,groupState,inputMap).toIterator.asJava


}
