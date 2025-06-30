package part2dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, column, expr, when}

object ColumnsAndExpressions {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("DF Columns and Expressions")
      .config("spark.master", "local")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    val carsDF = spark.read
      .option("inferSchema", "true")
      .json("src/main/resources/data/cars.json")

    // Columns
    val firstColumn = carsDF.col("Name")
    println(firstColumn)

    // selecting (projecting)
    val carNamesDF = carsDF.select(firstColumn)
    carNamesDF.show()

    // various select methods
    import spark.implicits._
    carsDF
      .select(
        carsDF.col("Name"),
        col("Acceleration"),
        column("Weight_in_lbs"),
        'Year, // Scala Symbol, auto-converted to column
        $"Horsepower", // fancier interpolated string, returns a Column object
        expr("Origin") // EXPRESSION
      )
      .show()

    // select with plain column names
    carsDF.select("Name", "Year")

    // EXPRESSIONS
    val simplestExpression = carsDF.col("Weight_in_lbs")
    val weightInKgExpression = carsDF.col("Weight_in_lbs") / 2.2

    println(simplestExpression)
    println(weightInKgExpression)

    val carsWithWeightsDF = carsDF.select(
      col("Name"),
      col("Weight_in_lbs"),
      weightInKgExpression.as("Weight_in_kg"),
      expr("Weight_in_lbs / 2.2").as("Weight_in_kg_2")
    )

    carsWithWeightsDF.show()

    // selectExpr
    val carsWithSelectExprWeightsDF = carsDF.selectExpr(
      "Name",
      "Weight_in_lbs",
      "Weight_in_lbs / 2.2"
    )

    carsWithSelectExprWeightsDF.show()
    // DF processing

    // adding a column
    val carsWithKg3DF =
      carsDF.withColumn("Weight_in_kg_3", col("Weight_in_lbs") / 2.2)

    carsWithKg3DF.show()

    // renaming a column
    val carsWithColumnRenamed =
      carsDF.withColumnRenamed("Weight_in_lbs", "Weight in pounds")
    // careful with column names
    carsWithColumnRenamed.selectExpr("`Weight in pounds`").show()
    carsWithColumnRenamed.selectExpr("`Name`").show()
    // remove a column
    carsWithColumnRenamed.drop("Cylinders", "Displacement").show()

    // filtering
    println("\n\n\nFiltering USA cars using filter\n\n\n")
    carsDF.filter(col("Origin") =!= "USA").show()

    println("\n\n\nFiltering USA cars using where\n\n\n")
    carsDF.where(col("Origin") =!= "USA").show()

    // filtering with expression strings
    println("\n\n\nFiltering USA cars using expression strings\n\n\n")
    carsDF.filter("Origin = 'USA'").show()

    // chain filters
    println("\n\n\nChained filters\n\n\n")
    carsDF
      .filter(col("Origin") === "USA")
      .filter(col("Horsepower") > 150)
      .show()

    carsDF.filter(col("Origin") === "USA" and col("Horsepower") > 150)

    carsDF.filter("Origin = 'USA' and Horsepower > 150")

    // unioning = adding more rows
    val moreCarsDF = spark.read
      .option("inferSchema", "true")
      .json("src/main/resources/data/more_cars.json")

    println("\n\n\nUnion of two DataFrames\n\n\n")
    carsDF.union(moreCarsDF).show() // works if the DFs have the same schema

    // distinct values
    println("\n\n\nDistinct values of Origin\n\n\n")
    carsDF.select("Origin").distinct().show()

    val moivesDf = spark.read
      .format("json")
      .option("inferSchema", value = true)
      .json("src/main/resources/data/movies.json")

    moivesDf
      .select(moivesDf.col("Title"), column("IMDB_Rating"), $"IMDB_Votes")
      .show()

    val cleanedMoivesDf = moivesDf
      .withColumn(
        "US_DVD_Sales",
        when(col("US_DVD_Sales").isNull, 0)
          .otherwise((col("US_DVD_Sales")))
      )

    cleanedMoivesDf
      .selectExpr(
        "Title",
        "US_Gross + Worldwide_Gross + US_DVD_Sales as Total_Profit"
      )
      .show()

    moivesDf
      .where("Major_Genre = 'Comedy' and IMDB_Rating > 6")
      .select("Title", "IMDB_Rating", "Major_Genre")
      .show()

    /** Exercises
      *
      * 1. Read the movies DF and select 2 columns of your choice
      * 2. Create another column summing up the total profit of the movies = US_Gross + Worldwide_Gross + DVD sales
      * 3. Select all COMEDY movies with IMDB rating above 6
      *
      * Use as many versions as possible
      */

    val moviesDF = spark.read
      .option("inferSchema", "true")
      .json("src/main/resources/data/movies.json")
    moviesDF.show()

    // 1
    val moviesReleaseDF = moviesDF.select("Title", "Release_Date")
    val moviesReleaseDF2 = moviesDF.select(
      moviesDF.col("Title"),
      col("Release_Date"),
      $"Major_Genre",
      expr("IMDB_Rating")
    )
    val moviesReleaseDF3 = moviesDF.selectExpr(
      "Title",
      "Release_Date"
    )

    // 2
    val moviesProfitDF = moviesDF.select(
      col("Title"),
      col("US_Gross"),
      col("Worldwide_Gross"),
      col("US_DVD_Sales"),
      (col("US_Gross") + col("Worldwide_Gross")).as("Total_Gross")
    )

    val moviesProfitDF2 = moviesDF.selectExpr(
      "Title",
      "US_Gross",
      "Worldwide_Gross",
      "US_Gross + Worldwide_Gross as Total_Gross"
    )

    val moviesProfitDF3 = moviesDF
      .select("Title", "US_Gross", "Worldwide_Gross")
      .withColumn("Total_Gross", col("US_Gross") + col("Worldwide_Gross"))

    // 3
    val atLeastMediocreComediesDF = moviesDF
      .select("Title", "IMDB_Rating")
      .where(col("Major_Genre") === "Comedy" and col("IMDB_Rating") > 6)

    val comediesDF2 = moviesDF
      .select("Title", "IMDB_Rating")
      .where(col("Major_Genre") === "Comedy")
      .where(col("IMDB_Rating") > 6)

    val comediesDF3 = moviesDF
      .select("Title", "IMDB_Rating")
      .where("Major_Genre = 'Comedy' and IMDB_Rating > 6")

    comediesDF3.show
  }
}
