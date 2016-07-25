package com.sparkafka.demo

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.v09.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}


object SparkKafkaConsumerDemo {

  def main(args: Array[String]) = {
    if (args.length < 2) {
      System.err.println("Usage: SparkKafkaConsumerDemo <brokers> <topics>.")
      System.exit(1)
    }

    val Array(brokers, topics) = args

    val sparkConf = new SparkConf()
      .setAppName(SparkKafkaConsumerDemo.getClass.getName)
      .set("spark.cores.max", "1")

    val ssc = new StreamingContext(sparkConf, Seconds(2))
    ssc.checkpoint("/tmp")

    val topicsSet = topics.split(",").toSet

    val kafkaParams = Map[String, String](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> brokers,
      ConsumerConfig.GROUP_ID_CONFIG -> "testGroup",
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG ->
        classOf[StringDeserializer].getName,
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG ->
        classOf[WikiDataDeserializer].getName,
      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "latest",
      ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> "false",
      "spark.kafka.poll.time" -> "1000")

    val wikiPagesStream: InputDStream[(String, WikiPage)] = {
      KafkaUtils.createDirectStream[String, WikiPage](ssc, kafkaParams, topicsSet)
    }

    val updateFunc = (newValues: Seq[Long], runningCount: Option[Long]) => {
      val newCount: Long = newValues.sum + runningCount.getOrElse(0L)
      Some(newCount)
    }

    val stats = wikiPagesStream.map(_._2)
      .map(page => page.domainCode -> page.countViews)
      .updateStateByKey(updateFunc)

    stats.transform(rdd => {
      rdd.sortBy(_._2, ascending = false)
    }).print(20)

    ssc.start()
    ssc.awaitTermination()

    ssc.stop(stopSparkContext = true, stopGracefully = true)
  }

}
