package org.controller

import org.scalatest.{FlatSpec,Matchers}
import org.controller.UDFOnSurvey._

class scalaTestForGenderFunction extends FlatSpec with Matchers {
  it should "hardcode to Male when make is present" in {
    pgender("make") should be ("Male")
  }

}
