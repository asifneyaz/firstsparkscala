import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
object WordCountRDD {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("WordCountRDD").setMaster("local")
    val sc = new SparkContext(conf)
    val rawData = sc.textFile("/Users/tech/codes/firstsparkscala/data/PrideandPrejudice.txt")
    val words = rawData.flatMap(line => line.split(" "))
    val wordCount = words.map(word => (word,1)).reduceByKey(_+_)
    wordCount.saveAsTextFile("/Users/tech/codes/firstsparkscala/output")
  }
}
