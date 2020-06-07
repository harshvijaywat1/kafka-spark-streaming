name := "KafkaSparkStream"
version := "0.1"
scalaVersion := "2.12.10"
val sparkVersion = "3.0.0-preview2"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion
   )   
// https://mvnrepository.com/artifact/org.apache.kafka/kafka
libraryDependencies += "org.apache.kafka" %% "kafka" % "2.5.0"
// https://mvnrepository.com/artifact/org.apache.spark/spark-streaming-kafka-0-10
libraryDependencies += "org.apache.spark" %% "spark-streaming-kafka-0-10" % "3.0.0-preview2"
// https://mvnrepository.com/artifact/org.apache.kafka/kafka-clients
libraryDependencies += "org.apache.kafka" % "kafka-clients" % "2.5.0"
// https://mvnrepository.com/artifact/org.apache.spark/spark-streaming
libraryDependencies += "org.apache.spark" %% "spark-streaming" % sparkVersion


