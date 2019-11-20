package serde


import java.util

import model.User
import org.apache.kafka.common.serialization.{Deserializer, Serializer}
import org.codehaus.jackson.map.ObjectMapper
import net.liftweb.json._

object UserSerDe{
  val deser = new UserSerDe
}
class UserSerDe extends Deserializer[User] {
  implicit val formats = DefaultFormats

  override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = super.configure(configs, isKey)

  override def close(): Unit = super.close()

  override def deserialize(topic: String, data: Array[Byte]): User = {
    parse((data.map(_.toChar)).mkString ).extract[User]
  }
}
