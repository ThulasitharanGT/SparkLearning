package org.controller.splittingTime

import java.sql.Timestamp


case class TripRecordCaseClass (StateID : String ,VehicleID: String ,Model: String ,StartTripTime:Timestamp, EndTripTime:Timestamp)
case class TripReferenceCaseClass (StateID : String ,State: String ,StartRideTime:Timestamp, EndRideTime:Timestamp)
