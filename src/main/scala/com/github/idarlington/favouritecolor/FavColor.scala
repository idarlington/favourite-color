package com.github.idarlington.favouritecolor

import java.util.Properties
import java.util.concurrent.TimeUnit

import com.lightbend.kafka.scala.streams.DefaultSerdes._
import com.lightbend.kafka.scala.streams.ImplicitConversions._
import com.lightbend.kafka.scala.streams.{KStreamS, KTableS, StreamsBuilderS}
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.streams.kstream.Printed
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}

object FavColor extends App {

  val config = new Properties()
  config.put(StreamsConfig.APPLICATION_ID_CONFIG, "color-app")
  config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
  config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

  val builder = new StreamsBuilderS()
  val colors: KTableS[String, String] = builder.table[String, String]("colour")

  val counted: KStreamS[String, Long] = colors
    .groupBy[String, Long]((_, value) => (value, 1))
    .count()
    .toStream

  // print to console
  counted.print(Printed.toSysOut[String, Long])

  counted.to("colour-count")

  val streams = new KafkaStreams(builder.build(), config)
  streams.start()

  sys.ShutdownHookThread {
    streams.close(10, TimeUnit.SECONDS)
  }

}
