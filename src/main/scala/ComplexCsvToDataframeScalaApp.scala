import org.apache.spark.sql.SparkSession

/**
 * CSV ingestion in a dataframe.
 *
 */
object ComplexCsvToDataframeScalaApp {

  /**
   * main() is your entry point to the application.
   *
   *
   */
  def main(args: Array[String]): Unit = {

    /**
     * The processing code.
     */
    // Creates a session on a local master
    val spark = SparkSession.builder
      .appName("Complex CSV to Dataframe")
      .master("local[*]")
      .getOrCreate

    println("Using Apache Spark v" + spark.version)

    // Reads a CSV file with header, called books.csv, stores it in a
    // dataframe
    val is_multiline = true
    val is_inferSchema = true
    val df = spark.read
      .format("csv")
      .option("header", "true")
      .option("multiline", is_multiline)
      .option("sep", ";")
      .option("quote", "*")
      .option("dateFormat", "MM/dd/yyyy")
      .option("inferSchema", is_inferSchema)
      .load("data/books.csv")

    println("Excerpt of the dataframe content:")

    // Shows at most 7 rows from the dataframe, with columns as wide as 90
    // characters
    df.show(50, 100)
    println("Dataframe's schema:")
    df.printSchema()

    spark.stop
  }

}
