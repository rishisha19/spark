package serde

import java.util

import model.PageViews
import net.liftweb.json._
import org.apache.flink.api.common.serialization.DeserializationSchema
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.kafka.common.serialization.Deserializer

object PageViewsSerDe{
  implicit val formats = DefaultFormats
  val deser = new PageViewsSerDe
}

class PageViewsSerDe extends Deserializer[PageViews] with DeserializationSchema[PageViews] {
  implicit val formats = DefaultFormats

  override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = super.configure(configs, isKey)

  override def close(): Unit = super.close()

  override def deserialize(topic: String, data: Array[Byte]): PageViews = {
    parse((data.map(_.toChar)).mkString ).extract[PageViews]
  }

  override def deserialize(message: Array[Byte]): PageViews = parse((message.map(_.toChar)).mkString ).extract[PageViews]

  override def isEndOfStream(nextElement: PageViews): Boolean = false

  override def getProducedType: TypeInformation[PageViews] = TypeInformation.of(classOf[PageViews])
}
