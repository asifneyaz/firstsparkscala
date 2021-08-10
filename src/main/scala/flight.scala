import org.apache.spark.sql.SparkSession
object flight {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("flight")
      .master("local[*]")
      .getOrCreate()
    val flightdata = spark
      .read
      .option("inferSchema", "true")
      .option("header", "true")
      .csv("data/flight-data/csv/2015-summary.csv")
    flightdata.show(3)
  }

}
