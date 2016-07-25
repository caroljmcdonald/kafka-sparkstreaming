package com.sparkafka.demo

import java.util

import org.apache.kafka.common.serialization.{Deserializer, Serializer}
import spray.json._

/*
* https://wikitech.wikimedia.org/wiki/Analytics/Data/Pagecounts-raw
* */
case class WikiPage(domainCode: String, pageTitle: String, countViews: Long, responseSize: Long) extends Serializable

object WikiPage {
  def fromString(input: String): WikiPage = {
    val tokens = input.trim.split("\\s")

    /** Ignore mobile domains */
    val code = if (tokens(0).endsWith(".mw")) {
      tokens(0).split(".mw").head
    } else {
      tokens(0)
    }

    val title = tokens(1)
    val count = tokens(2).toLong
    val responseSize = tokens(3).toLong

    WikiPage(code, title, count, responseSize)
  }
}

object JsonProtocol extends DefaultJsonProtocol {
  implicit val pageFormat = jsonFormat4(WikiPage.apply)
}

class WikiPageSerializer extends Serializer[WikiPage] {
  import JsonProtocol._

  override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = { /* NOP */ }

  override def serialize(topic: String, data: WikiPage): Array[Byte] = {
    data.toJson.compactPrint.getBytes()
  }

  override def close(): Unit = { /* NOP */ }
}

class WikiDataDeserializer extends Deserializer[WikiPage] {
  import JsonProtocol._

  override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = { /* NOP */ }

  override def close(): Unit = { /* NOP */ }

  override def deserialize(topic: String, data: Array[Byte]): WikiPage = {
    new String(data).parseJson.convertTo[WikiPage]
  }
}