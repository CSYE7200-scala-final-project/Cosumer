import org.apache.spark.ml.classification.RandomForestClassificationModel

object predictor extends App {

  // can't load the model from local machine
//  val spark = SparkConfig("local[2]", "Covid19Twitter")
//  val session = spark.session
//
//  val model = RandomForestClassificationModel.load("data/model")
//
//  val df = spark.readJson("data/input").select("timestamp", "text-final")
//  val cleanedDf = spark.readJson("data/tagOutput")
//
//  val extractor = featureExtractor("text-final", cleanedDf)
//  val dataset = extractor.rescaledData(df)
//
//  val prediction = model.transform(dataset)
//  prediction.show()

}
