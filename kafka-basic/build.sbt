name := "kafka-basic"
version := "0.1"
scalaVersion := "2.13.4"

val kafkaVersion = "2.4.0"
val log4jVersion = "2.4.1"
resolvers ++= Seq(
  "bintray-spark-packages" at "https://dl.bintray.com/spark-packages/maven",
  "Typesafe Simple Repository" at "https://repo.typesafe.com/typesafe/simple/maven-releases",
  "MavenRepository" at "https://mvnrepository.com",
  "confluent" at "https://packages.confluent.io/maven/"
)

/*
  Beware that if you're working on this repository from a work computer,
  corporate firewalls might block the IDE from downloading the libraries and/or the Docker images in this project.
 */
libraryDependencies ++= Seq(
  // logging
  "org.apache.logging.log4j" % "log4j-api" % log4jVersion,
  "org.apache.logging.log4j" % "log4j-core" % log4jVersion,

  // kafka
  "org.apache.kafka" %% "kafka" % kafkaVersion,
  "org.apache.kafka" % "kafka-streams" % kafkaVersion,

  "org.scalatest" %% "scalatest" % "3.0.8" % Test,
  "io.confluent"% "kafka-avro-serializer"% "5.0.0",
  "io.confluent"% "kafka-schema-registry-client" %"5.0.0"
)