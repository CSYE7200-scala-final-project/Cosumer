import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe

object KafkaConsumer extends App{

  val conf = new SparkConf()
    .setMaster("local[2]")
    .setAppName("Covid19Twitter")

  val streamingContext = new StreamingContext(conf, Seconds(1))
  println("here")
  val kafkaParams = Map[String, Object](
    "bootstrap.servers" -> "localhost:9092",
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    "group.id" -> "test_group",
    "auto.offset.reset_config" -> "earliest",
    "enable.auto.commit_config" -> (false: java.lang.Boolean)
  )

  val topics = Array("tweets_of_Coronavirus")
  val tweets = KafkaUtils.createDirectStream[String, String](
    streamingContext,
    PreferConsistent,
    Subscribe[String, String](topics, kafkaParams)
  )
  println(tweets.count())
  tweets.map(record => (println(record.key), println(record.value)))
  println("after reading")
}
