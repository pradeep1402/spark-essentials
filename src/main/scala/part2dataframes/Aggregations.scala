package part2dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Aggregations {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Aggregations and Grouping")
      .config("spark.master", "local")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    val moviesDF = spark.read
      .option("inferSchema", "true")
      .json("src/main/resources/data/movies.json")

    // counting
    moviesDF
      .select(count(col("Major_Genre")))
      .show() // all the values except null
    moviesDF.selectExpr("count(Major_Genre)").show()

    // counting all
    moviesDF
      .select(count("*"))
      .show() // count all the rows, and will INCLUDE nulls

    // counting distinct
    moviesDF.select(countDistinct(col("Major_Genre"))).show()

    // approximate count
    moviesDF.select(approx_count_distinct(col("Major_Genre"))).show()

    // min and max
    moviesDF.select(min(col("IMDB_Rating"))).show()
    moviesDF.selectExpr("min(IMDB_Rating)").show()

    // sum
    moviesDF.select(sum(col("US_Gross"))).show()
    moviesDF.selectExpr("sum(US_Gross)").show()

    // avg
    moviesDF.select(avg(col("Rotten_Tomatoes_Rating"))).show()
    moviesDF.selectExpr("avg(Rotten_Tomatoes_Rating)").show()

    // data science
    moviesDF
      .select(
        mean(col("Rotten_Tomatoes_Rating")),
        stddev(col("Rotten_Tomatoes_Rating"))
      )
      .show()

    // Grouping

    moviesDF
      .groupBy(col("Major_Genre")) // includes null
      .count()
      .show() // select count(*) from moviesDF group by Major_Genre

    moviesDF
      .groupBy(col("Major_Genre"))
      .avg("IMDB_Rating")
      .show()

    moviesDF
      .groupBy(col("Major_Genre"))
      .agg(
        count("*").as("N_Movies"),
        avg("IMDB_Rating").as("Avg_Rating")
      )
      .orderBy(col("Avg_Rating"))
      .show()

    moviesDF
//      .fill(0, Seq("US_Gross", "Worldwide_Gross", "US_DVD_Sales"))
      .selectExpr("US_Gross + Worldwide_Gross + US_DVD_Sales as Total_Revenue")
      .select(sum(col("Total_Revenue")))
      .show()

    moviesDF.select(countDistinct(col("Director"))).show()

    moviesDF.select(mean("US_Gross"), stddev("US_Gross")).show()

    moviesDF
      .groupBy(col("Director"))
      .agg(
        avg(col("IMDB_Rating")).as("Avg_Rating"),
        sum(col("US_Gross")).as("Total_US_Gross")
      )
      .show()

    /** Exercises
      *
      * 1. Sum up ALL the profits of ALL the movies in the DF
      * 2. Count how many distinct directors we have
      * 3. Show the mean and standard deviation of US gross revenue for the movies
      * 4. Compute the average IMDB rating and the average US gross revenue PER DIRECTOR
      */

    // 1
    moviesDF
      .select(
        (col("US_Gross") + col("Worldwide_Gross") + col("US_DVD_Sales"))
          .as("Total_Gross")
      )
      .select(sum("Total_Gross"))
      .show()

    // 2
    moviesDF
      .select(countDistinct(col("Director")))
      .show()

    // 3
    moviesDF
      .select(
        mean("US_Gross"),
        stddev("US_Gross")
      )
      .show()

    // 4
    moviesDF
      .groupBy("Director")
      .agg(
        avg("IMDB_Rating").as("Avg_Rating"),
        sum("US_Gross").as("Total_US_Gross")
      )
      .orderBy(col("Avg_Rating").desc_nulls_last)
      .show()

  }
}
