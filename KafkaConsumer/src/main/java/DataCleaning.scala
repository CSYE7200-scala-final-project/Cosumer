import java.util.Properties

import org.apache.spark.{SparkConf, sql}
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.sql.functions.{trim, lower, split,regexp_replace}
import org.apache.spark.sql.functions
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.feature.StopWordsRemover
import scala.collection.mutable
import scala.io.Source
import java.io.File
import org.slf4j.LoggerFactory


object DataCleaning extends  App with Context {


    val logger = LoggerFactory.getLogger(getClass.getSimpleName)
    logger.info("properties file loaded" )
    val propertiesFile = getClass.getResource("application.properties")
    val properties: Properties = new Properties()

    if (propertiesFile != null) {
      val source = Source.fromURL(propertiesFile)
      properties.load(source.bufferedReader())
      logger.info("properties file loaded" )
    }
    else {
      logger.error("properties file cannot be loaded at path ")
    }

    val inputFileFormat = properties.getProperty("inputFileformat")
    val inputFilePath   = properties.getProperty("inputFilepath")

    logger.info(inputFilePath + " " + inputFileFormat)

    if (directoryPresent(inputFilePath)) {

      val originalDf = readFile(inputFileFormat, inputFilePath)
       println("1")
      val textCleanedDF = originalDf.withColumn("text", regexp_replace(originalDf("text"), s"""[^ 'a-zA-Z0-9@#%&]""", ""))
      println("2")
      val filteredDf = textCleanedDF.filter(textCleanedDF("text").substr(1, 2) =!= "RT")
      println("3")
      val df1 = filteredDf.withColumn("text", trim(filteredDf("text")))
      println("4")
      val df2 = df1.withColumn("text",lower(df1("text")))
      println("5")
      val df3 = df2.withColumn("text",regexp_replace(df2("text"),"@[a-zA-Z]+" ,""))
      val df44 = df3.withColumn("text",regexp_replace(df3("text"),"""" +""" ,""))
      println("6")

      println("7")

      println("8")
      val entity = """&(amp|lt|gt|quot);"""
      val urlStart1 = """(https?://|www\.)"""
      val commonTLDs = """(com|co\.uk|org|net|info|ca|ly|mp|edu|gov)"""
      val urlStart2 = """[A-Za-z0-9\.-]+?\.""" + commonTLDs + """(?=[/ \W])"""
      val urlBody = """[^ \t\r\n<>]*?"""
      val punctChars = """['“\".?!,:;]"""
      val urlExtraCrapBeforeEnd = "(" + punctChars + "|" + entity + ")+?"
      val urlEnd = """(\.\.+|[<>]|\s|$)"""
      val url = """\b(""" + urlStart1 + "|" + urlStart2 + ")" + urlBody + "(?=(" + urlExtraCrapBeforeEnd + ")?" + urlEnd + ")"

      val cleanedDf = df44.withColumn("text", regexp_replace(df44("text"), url, ""))


      val df4 = cleanedDf.withColumn("text",split(cleanedDf("text")," "))

      val removedStopWordsDf = removeStopWords(df4,"text")

      removedStopWordsDf.createOrReplaceTempView("coviddata")

      val newdf3 = sparkSession.sql("SELECT *  FROM coviddata")

      newdf3.show(false)
      newdf3.write.json("output")
      newdf3.printSchema()

    }else {
      logger.info("Input file not present")
    }


  def removeStopWords(inputDF : sql.DataFrame,columnname :String)   = {

    val remover = new StopWordsRemover()
      .setInputCol(columnname)
      .setOutputCol(columnname +"-final")

    remover.transform(inputDF)
  }

  def readFile(format :String,path :String)   = {
    sparkSession.read.format(format).load(path)
  }


  def directoryPresent(path :String)   = {
      val d = new File(path)
    print(d.exists())
    print(d.isDirectory)
      if (d.exists && d.isDirectory) {
        true
      }else {
        false
      }
    }


}
