package model

import scala.collection.mutable

case class User(registertime: Long, userid: String, regionid: String, gender: String)

case class PageViews(viewtime: Long, userid: String, pageid: String)

case class Views(viewtime: Long, userid: String, pageid: String, sum: Long, users: mutable.Set[String], gender: String)

case class TopViews(pageid: String, gender: String, viewtime: Long, distinctUserCount: Long)
