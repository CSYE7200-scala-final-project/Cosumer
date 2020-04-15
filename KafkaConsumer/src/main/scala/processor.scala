import java.util.Properties

import DataCleaning.{getClass, logger, properties, propertiesFile}
import breeze.linalg.where
import org.apache.spark.ml.feature.{CountVectorizer, CountVectorizerModel, HashingTF, IDF, LabeledPoint, Tokenizer}
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{FloatType, StructField, StructType}
import org.slf4j.LoggerFactory

import scala.collection.mutable._
import scala.io.Source

//for testing
object processor extends App {

  val logger = LoggerFactory.getLogger(getClass.getSimpleName)

  val propertiesFile = getClass.getResource("application.properties")
  val properties: Properties = new Properties

  if (propertiesFile != null) {
    val source = Source.fromURL(propertiesFile)
    properties.load(source.bufferedReader())
    logger.info("properties file loaded" )
  }
  else {
    logger.error("properties file cannot be loaded at path ")
  }

  val path = properties.getProperty("inputCleanedFilePath")

  val spark = SparkConfig("local[2]", "Covid19Twitter")
  val df = spark.readJson(path).select("hashtag", "timestamp", "text-final")

  val result= df.select("text-final").rdd.map(_.getAs[Seq[String]]("text-final").mkString(","))
  val topKeys = featureExtractor.topNKeys(result, 100)
  topKeys.foreach(println)

  val initialDf = spark.emptyDf(topKeys)

  initialDf.show

  val extractor = featureExtractor("text-final", df)
  extractor.rescaledData.select("features").foreach(println(_))

}
