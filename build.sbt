name := "spark-kafka-demo"

version := "1.0"

scalaVersion := "2.10.6"

scalacOptions += "-target:jvm-1.7"

resolvers += "MapR Repo" at "http://repository.mapr.com/maven/"

libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-streaming-kafka-v09_2.10" % "1.5.2-mapr-1602" % "provided",
  "org.apache.spark" % "spark-streaming-kafka-producer_2.10" % "1.5.2-mapr-1602" % "provided",
  "org.apache.spark" % "spark-core_2.10" % "1.5.2" % "provided",
  "org.apache.spark" % "spark-streaming_2.10" % "1.5.2" % "provided",
  "org.apache.kafka" % "kafka-clients" % "0.9.0.0-mapr-1602" % "provided" excludeAll(
    ExclusionRule("org.scala-lang"),
    ExclusionRule("org.slf4j"),
    ExclusionRule("net.jpountz.lz4")),
  "org.apache.kafka" % "kafka_2.10" % "0.9.0.0" excludeAll(
    ExclusionRule("org.apache.zookeeper"),
    ExclusionRule("org.scala-lang")),
  "io.spray" %% "spray-json" % "1.3.2"
)
