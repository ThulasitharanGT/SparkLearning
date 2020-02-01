package org.controller.kafkaStreamExample

import sys.process._
import util.control.Breaks._

object systemCommandRunnerForShellScript {
  def main(args: Array[String]): Unit = {
    val inputMap:collection.mutable.Map[String,String]=collection.mutable.Map[String,String]()
    for (arg <- args)
      {
        val keyPart=arg.split("=",2)(0)
        val valPart=arg.split("=",2)(1)
        inputMap.put(keyPart,valPart)
      }
    val scriptsNamesWithParam=inputMap("scriptsNamesWithParam").replace("~"," ").split(",")
    val basePath=inputMap("basePath")// /home/raptor/IdeaProjects/SparkLearning/kafkaStreamingPipelineScripts/
    var result:Int= (-1)
  breakable {
    for (scriptNamesWithParam <- scriptsNamesWithParam) {
      val shellScriptCommand = "sh "+basePath + scriptNamesWithParam
      println("executing" + shellScriptCommand)
      result = shellScriptCommand !; // if we take thi ; then an error arise's. sys command spl char is not getting recognized as EOF
      result match {
        case 0 => println("Execution Successful ====> " + shellScriptCommand)
        case _ => break
      }
    }
    println("===================================> All Script's execution were successful")
  }
  }
}
