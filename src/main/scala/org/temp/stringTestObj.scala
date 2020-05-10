package temp

object stringTestObj {

  def main(args: Array[String]): Unit = {
    GRP_DIVN_NB_GeneratorFunction("AXTUIOPEHJLLJNNJJK", 127)
    GRP_DIVN_NB_GeneratorFunction("AUTUIOPEHJLLJNNJJK", 127)
    GRP_DIVN_NB_GeneratorFunction("AYTUIOPEHJLLJNNJJK", 17)
    GRP_DIVN_NB_GeneratorFunction("", 127)

  }

  def GRP_DIVN_NB_GeneratorFunction(MCEFCLM_GROUP_ID: String, SRC_SYS_CD: Int) = {
    val secondLetter = MCEFCLM_GROUP_ID.substring(1, 2)
    var resultString = ""
    secondLetter.toUpperCase match {
      case value if ((value == "X" || value == "Y") && SRC_SYS_CD == 127) => resultString = MCEFCLM_GROUP_ID.substring(8, 11)
      case value if (!(value == "X" || value == "Y")) => resultString = MCEFCLM_GROUP_ID.substring(7, 10)
      case _ => resultString = null
    }
    resultString
  }
}