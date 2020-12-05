package org.controller.slickEG.controller

import org.controller.slickEG.caseClass.{address, jdbcConnectionDetails}
import org.controller.slickEG.constants.projectConstants._
import org.controller.slickEG.utils.connectionHelper._
import org.controller.slickEG.DAO.addressesTable

import scala.concurrent.Await
import scala.concurrent.duration.Duration
//import org.controller.slickEG.configs.DBConfiguration
//import slick.backend.DatabaseConfig
//import slick.driver.JdbcProfile


import slick.driver.MySQLDriver.api._ // for Session
//import javax.sql.DataSource
//import slick.profile.BasicProfile

object helloSlick  {
def main(args:Array[String]):Unit={


  val hikariConfig=getHikariConfig
  val inputMap=collection.mutable.Map[String,String] ()
  val dbConfig=jdbcConnectionDetails("jdbc:mysql://localhost:3306/testSlick?user=raptor&password=","raptor","","com.mysql.jdbc.Driver")
  inputMap.put(jdbcMaxConnectionPoolSizeConfig,"2")
  inputMap.put(jdbcIdleTimeoutConfig,"11000")
  applyHikariConfig(hikariConfig,dbConfig,inputMap)
  val dataSource=getDatasource(hikariConfig)
  val database=Database.forDataSource(dataSource)

  val slickSession =database.createSession()
  val addressTable=TableQuery[addressesTable]((tag:Tag) => new addressesTable(tag,Some("testSlick"),"testSlick.address")) // new addressesTable(tag,Some("testSlick"),"address"))
  Await.result(slickSession.database.run(addressTable+= address(Some(1),"addLine1","city1","postalCode1",3)),Duration.Inf)


  /*  val hikariConfig=getHikariConfig
  val inputMap=collection.mutable.Map[String,String] ()
  val dbConfig=jdbcConnectionDetails("jdbc:mysql://localhost:3306/testSlick?user=raptor&password=","raptor","","com.mysql.jdbc.Driver")
  inputMap.put(jdbcMaxConnectionPoolSizeConfig,"2")
  inputMap.put(jdbcIdleTimeoutConfig,"11000")
  applyHikariConfig(hikariConfig,dbConfig,inputMap)
  val hikariDataSource = getDatasource(hikariConfig)
 // val hikariDataSourceConnection=getConnection(hikariDataSource)
  val sessionInitiation= sessionInitiator(hikariDataSource,2)
  val address1=address(Some(1),"address1","city1","postalCode1",30)
  val session=sessionInitiation.createSession()
  insert(address1,session)*/
}
}
