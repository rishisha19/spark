package serde

import java.util

import model.TopViews
import net.liftweb.json._
import org.apache.kafka.common.serialization.Serializer

import net.liftweb.json.Serialization

object TopViewsSerDe {
  val ser = new TopViewsSerDe
}

class TopViewsSerDe extends Serializer[TopViews] {
  implicit val formats = DefaultFormats

  override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = super.configure(configs, isKey)

  override def close(): Unit = super.close()

  override def serialize(topic: String, data: TopViews): Array[Byte] = {
    if(data == null)
      null
    else {
      Serialization.write(data).getBytes()
    }
  }
}
