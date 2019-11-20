package serde

import java.util

import model.PageViews
import net.liftweb.json._
import org.apache.kafka.common.serialization.Deserializer

object PageViewsSerDe{
  val deser = new PageViewsSerDe
}

class PageViewsSerDe extends Deserializer[PageViews] {
  implicit val formats = DefaultFormats

  override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = super.configure(configs, isKey)

  override def close(): Unit = super.close()

  override def deserialize(topic: String, data: Array[Byte]): PageViews = {
    parse((data.map(_.toChar)).mkString ).extract[PageViews]
  }
}
