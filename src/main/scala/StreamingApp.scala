import java.util.concurrent.Future

import client.KafkaClient
import config.Config
import model.{PageViews, TopViews, User}
import org.apache.kafka.clients.producer.{Callback, ProducerConfig, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.apache.spark.streaming.{Minutes, Seconds}
import serde.{PageViewsSerDe, TopViewsSerDe, UserSerDe}

object StreamingApp {

  def main(args: Array[String]): Unit = {
     println(Config.kafkaServerAddress)

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> Config.kafkaServerAddress,
      "group.id" -> "stream_group",
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> (true: java.lang.Boolean)
    )

    val producer = KafkaClient.getJsonKafkaProducer(
      kafkaParams + (ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG -> classOf[StringSerializer],
        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG -> classOf[TopViewsSerDe], ProducerConfig
        .TRANSACTIONAL_ID_CONFIG -> "stream-id-1"))


    val rawUserDataStream = KafkaClient.consumeJsonDataStream[User](Config.usersTopic,
      kafkaParams + ( "key.deserializer" -> classOf[StringDeserializer], "value.deserializer" -> classOf[UserSerDe]))
      .map(record => (record.value().userid, record.value()))

    val pageViewsDataStream = KafkaClient.consumeJsonDataStream[PageViews](Config.pageViewsTopic,
      kafkaParams + ( "key.deserializer" -> classOf[StringDeserializer], "value.deserializer" -> classOf[PageViewsSerDe]))
      .map(record => (record.value().userid, record.value()))

    val userAndPgeViewWindowedStream = rawUserDataStream.join(pageViewsDataStream)
        .map(a => ((a._2._2.pageid,a._2._1.gender), (a._2._1.userid, a._2._2.viewtime)))

    val distinctUserForPageStream = userAndPgeViewWindowedStream.map(a => (a._1, a._2._1))
      .countByValue().map(a => (a._1._1, a._2))

    val viewTimeBasedOnPageidAndGenderStream = userAndPgeViewWindowedStream.map(a => (a._1, a._2._2))
      .reduceByKey(_ + _)
    producer.initTransactions()
    viewTimeBasedOnPageidAndGenderStream.join(distinctUserForPageStream).groupByKey()
      .window(Minutes(1), Seconds(10))
      .map(r => (r._1,r._2.foldLeft((0L, 0L)) { case ((accA, accB), (a, b)) => (accA + a, accB + b) }))
      .map(r => TopViews(r._1._1, r._1._2, r._2._1, r._2._2))
      .foreachRDD( rdd => {
        producer.beginTransaction()
        println("##########################################S")
        rdd.sortBy(_.viewtime, ascending = false).take(10)
          .foreach(topViews => {

            producer.send(new ProducerRecord(Config.topPagesTopic, topViews.pageid, topViews), new Callback {
              override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
                if (null == exception)
                  println("published to kafka")
                else {
                  exception.printStackTrace()
                  producer.abortTransaction()
                }
              }
            })
          })
        println("##########################################S")
        producer.commitTransaction()
      })
    KafkaClient.start()

    try {
      KafkaClient.awaitTermination()
      producer.close()
      println("*** streaming terminated")
    } catch {
      case e: Exception => {
        println("*** streaming exception caught in monitor thread")
      }
    }

    // stop Spark
    KafkaClient.stop()
  }

}
