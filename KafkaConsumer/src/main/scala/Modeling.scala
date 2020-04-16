import org.apache.spark.ml.classification.{RandomForestClassificationModel, RandomForestClassifier}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator

object Modeling extends App {

  val spark = SparkConfig("local[2]", "Covid19Twitter")
  val cleanedDf = spark.readJson("data/tagOutput")

  val extractor = featureExtractor("text-final", cleanedDf)
  //  extractor.rescaledData.show()
  //  extractor.rescaledData.foreach(println(_))

  val dataset = extractor.rescaledData(cleanedDf)
  //  dataset.foreach(println(_))

  val Array(trainingData, testData) = dataset.randomSplit(Array(0.7, 0.3))

  val rf = new RandomForestClassifier()
    .setLabelCol("emotion")
    .setFeaturesCol("features")
    .setNumTrees(8000)
    .setMaxDepth(20)
    //    .setMinInstancesPerNode(100)
    .setImpurity("entropy")
    .setSubsamplingRate(0.8)
    .setFeatureSubsetStrategy("auto")
    .setMaxBins(70)

  val model = rf.fit(dataset)
//  val predictions = model.transform(testData)
  //
  //  predictions.select("prediction", "emotion", "features").show(5)
  //
  //  // Select (prediction, true label) and compute test error.
  //  val evaluator = new MulticlassClassificationEvaluator()
  //    .setLabelCol("emotion")
  //    .setPredictionCol("prediction")
  //    .setMetricName("accuracy")
  //  val accuracy = evaluator.evaluate(predictions)
  //  println("Test Error = " + (1.0 - accuracy))
  //
  //  model.write.overwrite.save("data/model")

  val df = spark.readJson("data/input").select("timestamp", "text-final")

  val prediction = model.transform(dataset)
  prediction.show()

  prediction.write.json("data/prediction")
}
