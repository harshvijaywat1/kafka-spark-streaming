import org.apache.spark.streaming.Seconds
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.SparkContext._
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka010._


object KafkaStream{
def main(args : Array[String]){
val brokers = "localhost:9092"
val groupid = "GRP1"
val topics = "kafkastream"
val SparkConf = new SparkConf().setMaster("local[*]").setAppName("KafkaStream")
val ssc = new StreamingContext(SparkConf, Seconds(3))
val sc = ssc.sparkContext
sc.setLogLevel("OFF")

val topicSet = topics.split(",").toSet
val KafkaParams = Map[String, Object](
ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> brokers,  
ConsumerConfig.GROUP_ID_CONFIG -> groupid,
ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer] 
)
val messages = KafkaUtils.createDirectStream[String, String](
ssc, LocationStrategies.PreferConsistent, ConsumerStrategies.Subscribe[String, String](topicSet, KafkaParams)
) 
val line = messages.map(_.value)
val words = line.flatMap(_.split(" "))
val wordcounts = words.map(x => (x,1)).reduceByKey(_+_)
wordcounts.print()
ssc.start()
ssc.awaitTermination()

}
}
