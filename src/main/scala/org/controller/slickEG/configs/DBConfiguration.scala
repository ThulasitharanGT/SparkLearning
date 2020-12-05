package org.controller.slickEG.configs

import slick.backend.DatabaseConfig
import slick.driver.JdbcProfile

trait DBConfiguration {
  val config :DatabaseConfig[JdbcProfile] //= DatabaseConfig.forConfig[JdbcProfile]("slick-mysql")  //(System.getProperty("user.dir")+"/conf/applicationSlickEG.conf")
  implicit val db: JdbcProfile#Backend#Database = config.db
}