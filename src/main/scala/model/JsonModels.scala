package model

case class User(registertime: Long, userid: String, regionid: String, gender: String)

case class PageViews(viewtime: Long, userid: String, pageid: String, sum: Option[Long], users: Option[String], gender: Option[String])

case class TopViews(pageid: String, gender: String, viewtime: Long, distinctUserCount: Long)
