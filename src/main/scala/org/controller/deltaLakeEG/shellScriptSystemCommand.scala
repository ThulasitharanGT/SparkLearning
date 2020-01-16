package org.controller.deltaLakeEG

import sys.process._
import util.control.Breaks._
object shellScriptSystemCommand {
  def main(args: Array[String]): Unit = {
    val inputMap:collection.mutable.Map[String,String]=collection.mutable.Map[String,String]()
    for (arg <- args)
      {
        val keyPart=arg.split("=",2)(0)
        val valuePart=arg.split("=",2)(1)
        inputMap.put(keyPart,valuePart)
      }
    //val basePath="/home/raptor/IdeaProjects/SparkLearning/temp/"
    val basePath=inputMap("basePath")
    val scriptNameList=inputMap("scriptNameList").split(",")
    var result:Int=2
   try {
     for (scriptName <- scriptNameList) {
       println("executing script --------------> " + scriptName)
       val commandToRun = "sh " + basePath + scriptName
       result = commandToRun!;  // because of exclamation we need semi colon. else jar build fail's in REPL we don't need this
       if (result == 0)
         println(scriptName +" Excecution Success")
       if (result != 0)
         break
       //"ls "+basePath!
     }
     println("Scripts executed Successfully .......................................... ")
   }
    catch {
      case e:Exception => println(e.printStackTrace)
    }
    /* val tableCreationBronze="sh "+basePath+"tableCreationBronze.sh"
     val tableAppendingBronze_1="sh "+basePath+"tableAppendingBronze_1.sh"
     val tableAppendingBronze_2="sh "+basePath+"tableAppendingBronze_2.sh"
     val tableAppendingBronze_3="sh "+basePath+"tableAppendingBronze_3.sh"
     val tableUpdationBronze_1="sh "+basePath+"tableUpdationBronze_1.sh"
     val tableUpdationBronze_2="sh "+basePath+"tableUpdationBronze_2.sh"
     val tableCreationSilver="sh "+basePath+"tableCreationSilver.sh"
     val tableAppendingSilver_1="sh "+basePath+"tableAppendingSilver_1.sh"*/
   // var result:Int=2
   // result=tableCreationBronze!

  }

}
