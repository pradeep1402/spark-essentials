package part3typesdatasets

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object ManagingNulls extends App {

  val spark = SparkSession
    .builder()
    .appName("Managing Nulls")
    .config("spark.master", "local")
    .getOrCreate()

  spark.sparkContext.setLogLevel("WARN")

  val moviesDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")

  // select the first non-null value
  moviesDF
    .select(
      col("Title"),
      col("Rotten_Tomatoes_Rating"),
      col("IMDB_Rating"),
      coalesce(col("Rotten_Tomatoes_Rating"), col("IMDB_Rating") * 10)
    )
    .show()

  // checking for nulls
  moviesDF
    .select(col("Title"), col("Rotten_Tomatoes_Rating"))
    .where(col("Rotten_Tomatoes_Rating").isNull)
    .show()

  // nulls when ordering
  moviesDF
    .orderBy(col("IMDB_Rating").desc_nulls_last)
    .select("Title", "IMDB_Rating")
    .show()

  // removing nulls
  moviesDF
    .select("Title", "IMDB_Rating")
    .na
    .drop() // remove rows containing nulls

  // replace nulls
  moviesDF.na.fill(0, List("IMDB_Rating", "Rotten_Tomatoes_Rating"))
  moviesDF.na
    .fill(
      Map(
        "IMDB_Rating" -> 0,
        "Rotten_Tomatoes_Rating" -> 10,
        "Director" -> "Unknown"
      )
    )
    .show(false)

  // complex operations
  moviesDF
    .selectExpr(
      "Title",
      "IMDB_Rating",
      "Rotten_Tomatoes_Rating",
      "ifnull(Rotten_Tomatoes_Rating, IMDB_Rating * 10) as ifnull", // same as coalesce
      "nvl(Rotten_Tomatoes_Rating, IMDB_Rating * 10) as nvl", // same
      "nullif(Rotten_Tomatoes_Rating, IMDB_Rating * 10) as nullif", // returns null if the two values are EQUAL, else first value
      "nvl2(Rotten_Tomatoes_Rating, IMDB_Rating * 10, 0.0) as nvl2" // if (first != null) second else third
    )
    .show()
}
