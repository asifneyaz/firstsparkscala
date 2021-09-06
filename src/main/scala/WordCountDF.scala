import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object WordCountDF {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("WordCount")
      .master("local[*]")
      .getOrCreate
    val df = spark.sqlContext.read.text("/Users/tech/codes/SparkJourney/data/PrideandPrejudice.txt")
    val wordsDF = df.select(split(df("value")," ").alias("words"))
    val wordDF = wordsDF.select(explode(wordsDF("words")).alias("word"))
    val wordCountDF = wordDF.groupBy("word").count
    wordCountDF.orderBy(desc("count")).show(truncate=false)
  }
}
