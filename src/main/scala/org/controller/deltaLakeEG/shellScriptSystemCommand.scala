package org.controller.deltaLakeEG

import sys.process._
object shellScriptSystemCommand {
  def main(args: Array[String]): Unit = {
    val basePath="/home/raptor/IdeaProjects/SparkLearning/temp/"
    val tableCreationBronze="sh "+basePath+"tableCreationBronze.sh"
    val tableAppendingBronze_1="sh "+basePath+"tableAppendingBronze_1.sh"
    val tableAppendingBronze_2="sh "+basePath+"tableAppendingBronze_2.sh"
    val tableAppendingBronze_3="sh "+basePath+"tableAppendingBronze_3.sh"
    val tableUpdationBronze_1="sh "+basePath+"tableUpdationBronze_1.sh"
    val tableUpdationBronze_2="sh "+basePath+"tableUpdationBronze_2.sh"
    val tableCreationSilver="sh "+basePath+"tableCreationSilver.sh"
    val tableAppendingSilver_1="sh "+basePath+"tableAppendingSilver_1.sh"
    var result:Int=2
    result=tableCreationBronze!

  }

}
