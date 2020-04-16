import org.apache.spark.sql.Row
import org.scalatest.{FlatSpec, Matchers}

import scala.collection.mutable
import scala.collection.mutable.Seq

class MLSpec extends FlatSpec with Matchers {

  behavior of "readJson"

  it should "give correct number of columns" in {
    val spark = SparkConfig("local[2]", "Covid19Twitter")
    val df = spark.readJson("data/tagOutput")
    df.count shouldBe 2403
  }

  it should "get the right count of each word" in {
    val spark = SparkConfig("local[2]", "Covid19Twitter")
    val df = spark.readJson("data/input").select("timestamp", "text-final")
    val result1= df.select("text-final").rdd.map(_.getAs[Seq[String]]("text-final").mkString(","))
    val wordcount = featureExtractor.wordcount(result1)
    wordcount.first shouldBe ("#coronavirus", 2737)
  }

  it should "get the topN key of the document" in {
    val spark = SparkConfig("local[2]", "Covid19Twitter")
    val df = spark.readJson("data/input").select("timestamp", "text-final")
    val result1= df.select("text-final").rdd.map(_.getAs[Seq[String]]("text-final").mkString(","))
    val topKeys = featureExtractor.topNKeys(result1, 1)
    topKeys(0) shouldBe "#coronavirus"
  }

  it should "tranform the data into tf correctly" in {
    val spark = SparkConfig("local[2]", "Covid19Twitter")
    val cleanedDf = spark.readJson("data/tagOutput")

    val extractor = featureExtractor("text-final", cleanedDf)
    val featurized = extractor.featurizedData(cleanedDf)

    featurized.select("rawFeatures").first.toSeq(0).toString shouldBe "(779,[0,2,38,49,73,74,272,333,343,680,681,682,683,684,685,686,687,688],[1.0,2.0,1.0,1.0,1.0,1.0,1.0,2.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0])"
  }

  it should "tranform the data into tf-idf correctly" in {
    val spark = SparkConfig("local[2]", "Covid19Twitter")
    val cleanedDf = spark.readJson("data/tagOutput")

    val extractor = featureExtractor("text-final", cleanedDf)
    val rescaledData = extractor.rescaledData(cleanedDf)

    rescaledData.select("features").first.toSeq(0).toString shouldBe ()
  }

}
