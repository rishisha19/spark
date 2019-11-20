package client

import scala.collection.JavaConverters._
import model.TopViews
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}


object KafkaClient {

  var sparkContext: SparkContext = new SparkContext(new SparkConf().setAppName("KafkaStreamingApp").setMaster
  ("local[4]"))
  val sqlContext: SQLContext = SparkSession.builder().getOrCreate().sqlContext
  var sparkStreamingContext: StreamingContext = new StreamingContext(sparkContext, Seconds(1))
  var kafkaProducer: KafkaProducer[String, TopViews] = null
  def consumeJsonDataStream[A](topicName: String, kafkaParams: Map[String, Object])= {

    sparkStreamingContext.sparkContext.setLogLevel("ERROR")
    val topics = Array(topicName)
    KafkaUtils.createDirectStream(
      sparkStreamingContext,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, A](topics, kafkaParams)
    )
  }

  def getJsonKafkaProducer(kafkaParams: Map[String, Object]): KafkaProducer[String, TopViews] = {
    if (kafkaProducer == null )
      kafkaProducer = new KafkaProducer[String, TopViews](kafkaParams.asJava)

    kafkaProducer
  }

  def start() = sparkStreamingContext.start()

  def awaitTermination() = sparkStreamingContext.awaitTermination()

  def stop() = sparkContext.stop()

}
