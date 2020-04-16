import java.util.Properties

import org.apache.spark.ml.classification.{GBTClassificationModel, GBTClassifier, RandomForestClassificationModel, RandomForestClassifier}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.regression.GBTRegressor
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.LongType
import org.slf4j.LoggerFactory

import scala.collection.mutable._
import scala.io.Source

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

  val path = properties.getProperty("optimizedPath")

  val spark = SparkConfig("local[2]", "Covid19Twitter")

  val df = spark.readJson(path).select("timestamp", "text-final")
  val rd = spark.readJson("data/tags").select("emotion").rdd.map(r => r(0))
//  rd.foreach(println(_))
  val rdd_new = df.rdd.zip(rd).map(r => Row.fromSeq(r._1.toSeq ++ Seq(r._2)))

  val newdf = spark.session.createDataFrame(rdd_new, df.schema.add("emotion", LongType))
  val cleanedDf = newdf.filter(newdf.col("emotion").isNotNull) //.dropDuplicates("text-final")
  cleanedDf.show()
  cleanedDf.write.json("data/tagOutput")

  val result= newdf.select("text-final", "emotion").rdd
  result.foreach(println(_))

  val result1= df.select("text-final").rdd.map(_.getAs[Seq[String]]("text-final").mkString(","))
  val topKeys = featureExtractor.topNKeys(result1, 100)
  topKeys.foreach(println)

}
