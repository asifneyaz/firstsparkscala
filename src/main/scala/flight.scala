import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions
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
    flightdata.createOrReplaceTempView("flight_data_view")
    val sqlway = spark.sql(""" SELECT DEST_COUNTRY_NAME, COUNT(1) FROM flight_data_view GROUP BY DEST_COUNTRY_NAME""")
    sqlway.show(100)
    val maxcount = spark.sql("""SELECT max(COUNT) FROM flight_data_view""")
    maxcount.show(1)
    val dataframeway = flightdata
      .groupBy("DEST_COUNTRY_NAME")
      .count()
    dataframeway.show(100)
  }

}
