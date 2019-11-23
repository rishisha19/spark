package serde


import java.util

import model.User
import net.liftweb.json._
import org.apache.flink.api.common.serialization.DeserializationSchema
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.kafka.common.serialization.Deserializer
import org.apache.flink.streaming.api.scala._

object UserSerDe{
  implicit val formats = DefaultFormats
  val deser = new UserSerDe
}
class UserSerDe extends Deserializer[User] with DeserializationSchema[User]{
  implicit val formats = DefaultFormats

  override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = super.configure(configs, isKey)

  override def close(): Unit = super.close()

  override def deserialize(topic: String, data: Array[Byte]): User = {
    parse((data.map(_.toChar)).mkString ).extract[User]
  }

  override def deserialize(message: Array[Byte]): User =  {
    parse((message.map(_.toChar)).mkString ).extract[User]
  }

  override def isEndOfStream(nextElement: User): Boolean = false

  override def getProducedType: TypeInformation[User] = TypeInformation.of(classOf[User])
  // (classOf[User])
}
