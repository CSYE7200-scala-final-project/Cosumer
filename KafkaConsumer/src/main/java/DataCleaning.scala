import org.apache.spark.SparkConf
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.sql.functions.regexp_replace
import org.apache.spark.sql.SparkSession

import scala.collection.mutable
//import scala.reflect.io.File
import java.io.File

object DataCleaning extends  App with Context {


  val originalDf = readFile("json", "C:/Users/adwai/Documents/Scala Project/KafkaConsumer/tweets10-4-1586575460000")

  val textCleanedDF = originalDf.withColumn("text", regexp_replace(originalDf("text"), s"""[^ 'a-zA-Z0-9-@#%&]""", ""))

  val filteredDf = textCleanedDF.filter(textCleanedDF("text").substr(1,2) =!= "RT" )

  val entity = """&(amp|lt|gt|quot);"""
  val urlStart1 = """(https?://|www\.)"""
  val commonTLDs = """(com|co\.uk|org|net|info|ca|ly|mp|edu|gov)"""
  val urlStart2 = """[A-Za-z0-9\.-]+?\.""" + commonTLDs + """(?=[/ \W])"""
  val urlBody = """[^ \t\r\n<>]*?"""
  val punctChars = """['“\".?!,:;]"""
  val urlExtraCrapBeforeEnd = "(" + punctChars + "|" + entity + ")+?"
  val urlEnd = """(\.\.+|[<>]|\s|$)"""
  val url = """\b(""" + urlStart1 + "|" + urlStart2 + ")" + urlBody + "(?=(" + urlExtraCrapBeforeEnd + ")?" + urlEnd + ")"

  val cleanedDf = filteredDf.withColumn("text", regexp_replace(filteredDf("text"), url, ""))

  cleanedDf.createOrReplaceTempView("coviddata")

  val newdf3  = sparkSession.sql("SELECT *  FROM coviddata")

  newdf3.show(false)
  newdf3.write.json("output")
  newdf3.printSchema()


  def readFile(format :String,path :String)   = {

      sparkSession.read.format(format).load(path)
    
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
