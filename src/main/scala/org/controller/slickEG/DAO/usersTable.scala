package org.controller.slickEG.DAO

import org.controller.slickEG.configs.DBConfiguration
import org.controller.slickEG.caseClass.user


trait usersTable {
  this: DBConfiguration =>
  import config.driver.api._
  class Users(tag: Tag) extends Table[user](tag, "user") {
    // Columns
    def id = column[Int]("USER_ID", O.PrimaryKey, O.AutoInc)
    def email = column[String]("USER_EMAIL", O.Length(512))
    def firstName = column[Option[String]]("USER_FIRST_NAME", O.Length(64))
    def lastName = column[Option[String]]("USER_LAST_NAME", O.Length(64))
    // Indexes
    def emailIndex = index("USER_EMAIL_IDX", email, true)
    // Select
    def * = (id.?, email, firstName, lastName) <> (user.tupled, user.unapply)
  }
  val users = TableQuery[Users]
}
