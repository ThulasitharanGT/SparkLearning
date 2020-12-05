package org.controller.slickEG.DAO

import org.controller.slickEG.caseClass.address

import slick.driver.MySQLDriver.api._

  class addressesTable(tag: Tag,schemaName:Option[String],tableName:String) extends Table[address](tag, Some(""),"address") { // this table extended is from slick's internal framework
    // Columns
    def id = column[Int]("ADDRESS_ID")
    def addressLine = column[String]("ADDRESS_LINE")
    def city = column[String]("CITY")
    def postalCode = column[String]("POSTAL_CODE")
    // ForeignKey
    def userId = column[Int]("USER_ID")
    // Select
    def * = (id?,  addressLine, city, postalCode,userId) <>
      (address.tupled, address.unapply)
  }
 // var session:Session= _
 // val addresses = TableQuery[Addresses] // Represents entire table
  //def insert(Address:address,session:Session/*driver.MySQLDriver.backend.DatabaseDef*/) = session.database.run(addresses+=Address)   // means it inserts a address row into the  a address table. 'addresses' represents the entire address table.
//  def delete(addressId:String) = addresses.filter( _.id === addressId).delete  // means it inserts a address row into the  a address table. 'addresses' represents the entire address table.
 // def insertOrUpdate(Address:address) = {session=sessionInitiator(db) ;session.database.run(addresses.insertOrUpdate(Address))  } // means it inserts or updates a address row in the address table.


