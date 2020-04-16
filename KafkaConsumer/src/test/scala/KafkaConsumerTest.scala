import org.scalatest.{FlatSpec, Matchers}
import DataCleaning.directoryPresent
import DataCleaning.readFile
import KafkaConsumer.getKafkaParams
import org.apache.spark.sql


class KafkaConsumerTest extends FlatSpec with Matchers {

    behavior of "directoryPresent"
    it should "work When Directory is present" in {
      val x = directoryPresent("C:/coviddata/rawLayer")
      x shouldBe true
    }
    it should "work When Directory is  not present" in {
      val x = directoryPresent("C:/coviddata/rawLa")
      x shouldBe false
    }

    behavior of "readFile"
    it should "give correct number of column" in {
      val x = readFile("json", "C:/coviddata/rawLayer/tweet*")
      x.columns.size shouldBe (17)
    }

    it should "give correct number of rows" in {
      val x = readFile("json", "C:/coviddata/rawLayer/tweet*")
      x.count() shouldBe (20800)
    }

    behavior of "getKafkaParams"
    it should "work for correct object of getKafkaParams" in {
      val x = getKafkaParams("localhost:9092", "test_group","latest")
      x shouldBe a[Map[_, _]]
    }



}
