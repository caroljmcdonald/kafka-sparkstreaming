package com.sparkafka.demo

import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.producer._
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkKafkaProducerDemo {
  def main(args: Array[String]) = {

    if (args.length < 3) {
      System.err.println("Usage: SparkKafkaProducerDemo <brokers> <topic> <source-folder>.")
      System.exit(1)
    }

    val Array(brokers, topic, sourceFolder) = args

    val sparkConf = new SparkConf()
      .setAppName(SparkKafkaProducerDemo.getClass.getName)
      .set("spark.cores.max", "1")
    val ssc = new StreamingContext(sparkConf, Seconds(2))

    val producerConf = new ProducerConf(
      bootstrapServers = brokers.split(",").toList)

    val fileStream: DStream[String] = ssc.fileStream[LongWritable, Text, TextInputFormat](
      sourceFolder,
      (path: Path) => pathFilter(path),
      newFilesOnly = true
    ).map(_._2.toString)

    val wikiPagesStream: DStream[WikiPage] = fileStream.map(WikiPage.fromString)

    wikiPagesStream.sendToKafka[WikiPageSerializer](topic, producerConf)

    wikiPagesStream.count().print()

    ssc.start()
    ssc.awaitTermination()

    ssc.stop(stopSparkContext = true, stopGracefully = true)
  }

  private def pathFilter(path: Path): Boolean = {
    !path.getName().startsWith(".")
  }
}
