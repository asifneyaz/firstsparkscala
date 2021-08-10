import org.apache.spark.sql.SparkSession
object range{
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("range")
      .master("local[*]")
      .getOrCreate()
    val myRange = spark.range(1000).toDF("numbers")
    myRange.show(10)
  }
}