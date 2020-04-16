import java.util.Properties
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.slf4j.LoggerFactory
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import scala.io.Source

object KafkaConsumer extends App {

  val logger = LoggerFactory.getLogger(getClass.getSimpleName)

  val propertiesFile = getClass.getResource("application.properties")
  val properties: Properties = new Properties()

  if (propertiesFile != null) {
    val source = Source.fromURL(propertiesFile)
    properties.load(source.bufferedReader())
    println("properties file loaded")
  }
  else {
    println("properties file cannot be loaded at path ")
    System.exit(1)
  }

  val bootStrapServer = properties.getProperty("bootStrapServer")
  val consumerGroup = properties.getProperty("consumerGroup")
  val offsetSetting = properties.getProperty("auto.offset.reset")
  val topic = properties.getProperty("topic")


  val streamingContext = createStreamingContext

  println("Streaming Context created")

  val kafkaParams = getKafkaParams(bootStrapServer, consumerGroup, offsetSetting)

  val topics = Array(topic)

  val tweets = KafkaUtils.createDirectStream[String, String](
    streamingContext,
    PreferConsistent,
    Subscribe[String, String](topics, kafkaParams)
  )


  tweets.foreachRDD { rdd =>
    val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
    for (o <- offsetRanges) {
     println(s"${o.topic} ${o.partition} offsets: ${o.fromOffset} to ${o.untilOffset}")
    }
    val rddSize = rdd.count()
    println( "rdd size is " + rddSize)

  }

  val outputPath = properties.getProperty("outPutConsumer")
  tweets.foreachRDD { rdd =>
    if (!rdd.isEmpty()) {
      rdd.map(records => records.value()).saveAsTextFile(outputPath  + "/tweets" + DateTimeFormatter.ofPattern("yyyyMMddHHmmss").format(LocalDateTime.now))
      println("Output File written")
    }
  }

  startStreaming

  println("Streaming records started")




  def startStreaming = {

    streamingContext.start()
    streamingContext.awaitTermination()


  }

  def createStreamingContext = {

    val conf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("Covid19Twitter")

    new StreamingContext(conf, Seconds(10))

  }

  def getKafkaParams(bootStrapServer: String, consumerGroup: String, offsetSetting: String) = {
    Map[String, Object](
      "bootstrap.servers" -> bootStrapServer,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> consumerGroup,
      "auto.offset.reset" -> offsetSetting,
      "enable.auto.commit" -> (true: java.lang.Boolean)
    )

  }
}