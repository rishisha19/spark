import config.Config
import model.User
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Column, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.from_json
import org.apache.spark.sql.types.{DataType, StructType}
import serde.{PageViewsSerDe, UserSerDe}

object SparkStreamingApp {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("KafkaStreamingApp").setMaster("local[4]")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    val rawUserStream = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", Config.kafkaServerAddress)
      .option("subscribe", Config.usersTopic)
      .option("startingOffsets", "earliest")
      .load()

    val rawPageViewsStream = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", Config.kafkaServerAddress)
      .option("subscribe", Config.pageViewsTopic)
      .option("startingOffsets", "earliest")
      .load()

    spark.udf.register("userDeserialize",
      (topic: String, bytes: Array[Byte]) => UserSerDe.deser.deserialize(topic, bytes))
    spark.udf.register("pageViewDeserialize",
      (topic: String, bytes: Array[Byte]) => PageViewsSerDe.deser.deserialize(topic, bytes))

    val userJsonStream =rawUserStream
      .selectExpr("CAST(key AS STRING)", s"""userDeserialize(${Config.usersTopic}, value) AS user""")
      .select("userid", "gender")

    val pageViewJsonStream = rawPageViewsStream
      .selectExpr("CAST(key AS STRING)", s"""pageViewDeserialize(${Config.pageViewsTopic}, value) AS views""")
        .select("userid", "viewtime", "pageid")

    import spark.implicits._
    val rddJoined = userJsonStream.join(pageViewJsonStream, "userid")
      .as("userPageView")
      .toDF().agg(Map("viewtime" -> "sum", "userid" -> "count"))
        .orderBy()

    rddJoined
      /*.agg(Map("viewtime" -> "sum", "userid" -> "count"))
      .orderBy("")
      .groupBy("gender","pageid")*/


    spark.stop()

  }

}
