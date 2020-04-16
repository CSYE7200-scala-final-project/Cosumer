import java.io.File

import org.apache.spark.sql
import org.apache.spark.sql.types.{FloatType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

import scala.util.control.Breaks

case class SparkConfig(master: String, appName: String) {

  lazy val session = SparkSession
    .builder()
    .master(master)
    .appName(appName)
    .getOrCreate()

  def readJson(path: String) = session.read.json(path)

//  def emptyDf(columnNames: List[String]) = {
//    val schema = StructType(columnNames.map(fieldName => StructField(fieldName, FloatType, true)))
//    session.createDataFrame(session.sparkContext.emptyRDD[Row], schema)
//  }

}
