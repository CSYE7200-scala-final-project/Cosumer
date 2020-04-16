import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

trait Context {

  lazy val sparkConf = new SparkConf()
    .setAppName("COVID19 Data Transformation")
    .setMaster("local[*]")
    .set("spark.cores.max", "1")

  lazy val sparkSession = SparkSession
    .builder()
    .config(sparkConf)
    .getOrCreate()
}
