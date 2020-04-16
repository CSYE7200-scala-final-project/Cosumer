import java.io.File
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.Properties

import org.apache.spark.ml.feature.StopWordsRemover
import org.apache.spark.sql
import org.apache.spark.sql.functions.{lower, regexp_replace, split, trim}
import org.slf4j.LoggerFactory

import scala.io.Source
import scala.util.{Failure, Success, Try}


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
    val inputFiledir   = properties.getProperty("outPutConsumer")
    val inputfilePath  =  inputFiledir + "/" + properties.getProperty("inputFilePattern")

    logger.info(inputfilePath + " " + inputFileFormat)

    if (directoryPresent(inputFiledir)) {

      val originalDf = readFile(inputFileFormat, inputfilePath)

      logger.info("Input File read successfully")

      val finalString = preppareRegexPattern

      val textOriginal = originalDf.withColumn("text-original", originalDf("text"))

      val textCleanedDf = textOriginal.withColumn("text", regexp_replace(textOriginal("text"), finalString, ""))

      val changeExclamationDf = textCleanedDf.withColumn("text", regexp_replace(textCleanedDf("text"), "!", " ! "))

      val changeQuestionDf = changeExclamationDf.withColumn("text", regexp_replace(changeExclamationDf("text"), "\\?", " ? "))

      val removedSpaces = changeQuestionDf.withColumn("text", regexp_replace(changeQuestionDf("text"), " +", " "))

      val filteredDf = removedSpaces.filter(removedSpaces("text").substr(1, 2) =!= "RT")

      val trimeedDf = filteredDf.withColumn("text", trim(filteredDf("text")))

      val loweredDf = trimeedDf.withColumn("text",lower(trimeedDf("text")))

      val splittedDf = loweredDf.withColumn("text",split(loweredDf("text")," "))

      val removedStopWordsDf = removeStopWords(splittedDf,"text")

      removedStopWordsDf.createOrReplaceTempView("coviddata")

      val sqlDf = sparkSession.sql("SELECT *  FROM coviddata")

     // sqlDf.show(30,false)

      writeOutputJson(sqlDf)

      sqlDf.printSchema()

      logger.info("Application complete")

    }else {
      throw new Exception("Input File not present")
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

  def writeOutputJson(outputDf :sql.DataFrame)   = {

    val optimizedpath = properties.getProperty("optimizedPath")

    outputDf.write.json(optimizedpath +"/cleanedtweets"+ DateTimeFormatter.ofPattern("yyyyMMddHHmmss").format(LocalDateTime.now))

  }
  def stopSparkApplication()   =
    {
      sparkSession.stop()
      logger.info("Spark application stopped")
    }

  def preppareRegexPattern   = {
    val junkContent = """[^ 'a-zA-Z0-9@#%?!]"""
    val taggedPeople = """@(.*?)[\s]"""
    val entity = """&(amp|lt|gt|quot);"""
    val urlStart1 = """(https?://|www\.)"""
    val commonTLDs = """(com|co\.uk|org|net|info|ca|ly|mp|edu|gov)"""
    val urlStart2 = """[A-Za-z0-9\.-]+?\.""" + commonTLDs + """(?=[/ \W])"""
    val urlBody = """[^ \t\r\n<>]*?"""
    val punctChars = """['“\".?!,:;]"""
    val urlExtraCrapBeforeEnd = "(" + punctChars + "|" + entity + ")+?"
    val urlEnd = """(\.\.+|[<>]|\s|$)"""
    val url = """\b(""" + urlStart1 + "|" + urlStart2 + ")" + urlBody + "(?=(" + urlExtraCrapBeforeEnd + ")?" + urlEnd + ")"

    junkContent + "|" + taggedPeople   + "|" + url

  }


  def directoryPresent(path :String)   = {
    val d = new File(path)
      if (d.exists && d.isDirectory) {
        true
      }else {
        false
      }
    }


}
