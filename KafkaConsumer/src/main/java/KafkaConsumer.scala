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

  val streamingContext = new StreamingContext(conf, Seconds(10))

  println("here")

  val kafkaParams = Map[String, Object](
    "bootstrap.servers" -> "localhost:9092",
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    "group.id" -> "test_group",
    "auto.offset.reset" -> "latest",
    "enable.auto.commit" -> (true: java.lang.Boolean)
  )

  val topics = Array("tweets_of_Coronavirus")
  val tweets = KafkaUtils.createDirectStream[String, String](
    streamingContext,
    PreferConsistent,
    Subscribe[String, String](topics, kafkaParams)
  )


  tweets.foreachRDD { rdd =>
    // Get the offset ranges in the RDD
    val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
    for (o <- offsetRanges) {
      println(s"${o.topic} ${o.partition} offsets: ${o.fromOffset} to ${o.untilOffset}")
    }
  }

  println("check this")
  tweets.foreachRDD(r => {
    println("*** got an RDD, size = " + r.count())
  })

  tweets.map(record => ( record.value)).saveAsTextFiles("tweets10-4")

  streamingContext.start()
  streamingContext.awaitTermination()
}
