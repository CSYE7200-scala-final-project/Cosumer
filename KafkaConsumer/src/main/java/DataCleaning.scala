import DataCleaning.spark
import org.apache.spark.SparkConf
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.sql.functions.regexp_replace

object DataCleaning extends  App{

  import org.apache.spark.sql.SparkSession

  val spark = SparkSession
    .builder()
    .master("local[2]")
    .appName("Spark SQL basic example")
    .getOrCreate()

  // For implicit conversions like converting RDDs to DataFrames

  val df = spark.read.json("C:/Users/adwai/Documents/Scala Project/KafkaConsumer/tweets10-4-1586549770000")


  val newDf1 = df.withColumn("text", regexp_replace(df("text"), s"""[^ 'a-zA-Z0-9,.?!-@#%&]""", ""))

  //val newDf1 = newDf.withColumn("text", regexp_replace(newDf("text"), """[ \w+:\/{2}[\d\w-]+(\.[\d\w-]+)*(?:(?:\/[^\s/]*))*]""", ""))
  print("here1")
  newDf1.createOrReplaceTempView("coviddata")

  val newdf2  = spark.sql("SELECT text  FROM coviddata")
  // Displays the content of the DataFrame to stdout
  newdf2.show(false)
  newdf2.printSchema()
}
