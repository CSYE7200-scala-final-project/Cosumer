import org.apache.spark.ml.feature.{CountVectorizer, CountVectorizerModel, HashingTF, IDF}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql

case class featureExtractor(col: String, wordsData: sql.DataFrame) {

//  lazy val hashingTF = new HashingTF().setInputCol(col).setOutputCol("rawFeatures").setNumFeatures(2000)
  val model: CountVectorizerModel = new CountVectorizer()
    .setInputCol("text-final")
    .setOutputCol("rawFeatures")
    .setMinDF(2)
    .fit(wordsData)
  lazy val idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")
//  lazy val featurizedData = hashingTF.transform(wordsData)
  def featurizedData(data: sql.DataFrame) = model.transform(data)

  def rescaledData(data: sql.DataFrame) = {
    val fData = featurizedData(data)
    idf.fit(fData).transform(fData)
  }
}

object featureExtractor {

  def wordcount(data: RDD[String]) = data.flatMap(_.split(",")).map((_, 1)).reduceByKey(_ + _).sortBy(_._2, false)
  def topNKeys(data: RDD[String], n: Int) = wordcount(data).keys.collect.take(n).toList

}
