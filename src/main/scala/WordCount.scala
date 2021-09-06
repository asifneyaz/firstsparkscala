import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions


object WordCount {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("WordCount")
      .master("local[*]")
      .getOrCreate
    val df = spark.read.format("text").load("/Users/tech/codes/SparkJourney/data/PrideandPrejudice.txt")


  }
}
