package config

import com.typesafe.config.{Config, ConfigFactory}

object Config {

  val config: Config = ConfigFactory.load()
  val kafkaServerAddress: String = config.getString("kafka.bootstrap.servers")
  val usersTopic: String = config.getString("kafka.topic.users")
  val pageViewsTopic: String = config.getString("kafka.topic.pageviews")
  val topPagesTopic: String = config.getString("kafka.topic.top.pages")

}
