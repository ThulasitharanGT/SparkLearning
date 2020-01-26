package org.controller.deltaLakeEG
// for delta lake job alone
import sys.process._
import util.control.Breaks._


object systemCommandRunningForStreamingPipeline {
  def main(args: Array[String]): Unit = {
    println(args)
    val inputMap:collection.mutable.Map[String,String]=collection.mutable.Map[String,String]()
    for (arg <- args){
      val keyPart=arg.split("=",2)(0)
      val valPart=arg.split("=",2)(1)
      inputMap.put(keyPart,valPart)
    }
    val shellScriptsWithParam=inputMap("shellScriptsWithParam").replace("~"," ")
    println("shellScriptsWithParam ------------------> "+shellScriptsWithParam)
    val shellScriptsWithParamList=shellScriptsWithParam.split(",")
    breakable {
      for (shellScriptsWithParamCurrent <- shellScriptsWithParamList) {
        val result = "sh /home/raptor/IdeaProjects/KafkaGradleTest/kafkaPipeLineScripts/" + shellScriptsWithParamCurrent !;
        if (result!=0)
          break
        if (result ==0)
          println("sh /home/raptor/IdeaProjects/KafkaGradleTest/kafkaPipeLineScripts/" + shellScriptsWithParamCurrent +"  Execution Successful")
      }
      println("Scripts Executed Successfully ..................................................... ")
    }
  }
}