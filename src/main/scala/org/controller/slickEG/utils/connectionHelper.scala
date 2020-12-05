package org.controller.slickEG.utils

import slick.driver.MySQLDriver.api._
import com.zaxxer.hikari.{HikariConfig, HikariDataSource}

import org.controller.slickEG.constants.projectConstants._
import org.controller.slickEG.caseClass.jdbcConnectionDetails
object connectionHelper {
 def applyHikariConfig(hikariDataConfig:HikariConfig,jdbcParams:jdbcConnectionDetails,inputMap:collection.mutable.Map[String,String])=
  {
    hikariDataConfig.setJdbcUrl(jdbcParams.jdbcUrl)
    hikariDataConfig.setUsername(jdbcParams.connectionUser)
    hikariDataConfig.setPassword(jdbcParams.connectionPassword)
    hikariDataConfig.setDriverClassName(jdbcParams.driverName)
    hikariDataConfig.setMaximumPoolSize(inputMap(jdbcMaxConnectionPoolSizeConfig).toInt)
    hikariDataConfig.setIdleTimeout(inputMap(jdbcIdleTimeoutConfig).toInt)
    hikariDataConfig
  }
  def getHikariConfig()= new HikariConfig()
  def getDatasource(hikariConfig:HikariConfig)=new HikariDataSource(hikariConfig)
  def getConnection(HikariDataSource:HikariDataSource)=HikariDataSource.getConnection
  def sessionInitiator(hikariDataSource:HikariDataSource,maxConnections:Int)=Database.forDataSource(hikariDataSource)//,Some(maxConnections))
}
