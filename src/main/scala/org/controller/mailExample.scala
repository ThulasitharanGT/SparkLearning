package org.controller

import javax.mail._
import javax.mail.internet.InternetAddress
import javax.mail.internet.MimeMessage
object mailExample {

  def main(args: Array[String]): Unit = {
    val lineBr = "<br>"
    val fromMailId = "confidential.details.007@gmail.com"
    val smptHost="smtp.gmail.com"
    val toMailId ="gomgov07@gmail.com"// "govindarajmsme@gmail.com"
   // val toMailId = "driftking9696@gmail.com"
  //  val subject = "Invite for purchasing Ferrari SF90 Stradale"
   // val body = "Congratulations ," + lineBr + lineBr + " You have been selected as one of the few members who can buy a Ferrari SF90 Stradale " + lineBr + lineBr + "Thanks," + lineBr + "Ferrari Sales Team"
    val subject = "Invitation for internship program in NASA"
    val body = "Congratulations Mr Govindaraj M," + lineBr + lineBr + " Your son Master Jegadeesh G has been shortlisted by our institute NASA for his enormous performance in his academics. Based on the feedback given by his school Kendriya Vidayalaya IIT Chennai on his academics we have made a serious decision and have aggregated our results. It is our pleasure to inform you that Master Jagadeesh has been selected for summer internship program 2020. This Internship program will take place in NASA HQ Washington, D.C., United States from April 2020 to June 2020. Kindly initiate VISA process from your end and we hope that Master Jagadeesh has a Passport. The travel charges will not be bared by us, but we will provide Accommodation and food services for the internship period.  Kindly let us know if Master Jagadeesh are willing or not willing to take part in this Internship. If we dont get any response within 20 days of sending this E-Mail, we will assume that the candidate is not willing to participate and drop the processing of this application." + lineBr + lineBr + "Note : This is a confidential E-Mail and do not share this E-Mail with anyone other than School, Passport officers and Embassy. "+ lineBr + lineBr + "Thanks," + lineBr + "Nasa Internship Team"
    val prop = System.getProperties()
    prop.put("mail.smtp.host", smptHost)
    prop.put("mail.smtp.auth", "true")
    prop.put("mail.smtp.starttls.enable", "true")
    prop.put("mail.smtp.port", "587")
 // val auth=new Authenticator() {PasswordAuthentication getPasswordAuthentication() {return new PasswordAuthentication(fromMailId, "IAMTHEemperor")}}
    val session = Session.getDefaultInstance(prop,null)
    val message = new MimeMessage(session)
    val msg = new mailBeanClass(fromMailId, toMailId, subject, body)

    try {
      message.setFrom(new InternetAddress(msg.fromMailId))
      message.addRecipient(Message.RecipientType.TO, new InternetAddress(msg.toMailID))
      message.setSubject(msg.subject)
      //message.setHeader("name","value")
    //  message.setText(msg.body)
      message.setContent(msg.body,"text/html; charset=utf-8")

      val transport = session.getTransport("smtp")
      transport.connect(smptHost, fromMailId, "ajlvgqzkperftssx")
      transport.sendMessage(message,message.getAllRecipients)
      //      Transport.send(message)
    }
    catch {
      case exception: Exception => exception.printStackTrace()
    }
  }


}
