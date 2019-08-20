package org.controller

import org.junit._
import org.junit.Assert._
import org.controller.UDFOnSurvey._

class GenderFunctionJunit  {

  var pgenderTestVal:String=null
@Before
  def startingInitialization(): Unit =
  {
    pgenderTestVal="Me"
  }

  @Test
  def PgenderTestingScn1: Unit ={
    assertEquals("Others",pgender(pgenderTestVal))
  }

  @Test
  def PgenderTestingScn2: Unit ={
    pgenderTestVal="make"
    assertEquals("Male",pgender(pgenderTestVal))
  }


}
