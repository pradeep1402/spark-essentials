package part2dataframes

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.types._

object DataSources {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("Data Sources and Formats")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    val carsSchema = StructType(
      Array(
        StructField("Name", StringType),
        StructField("Miles_per_Gallon", DoubleType),
        StructField("Cylinders", LongType),
        StructField("Displacement", DoubleType),
        StructField("Horsepower", LongType),
        StructField("Weight_in_lbs", LongType),
        StructField("Acceleration", DoubleType),
        StructField("Year", DateType),
        StructField("Origin", StringType)
      )
    )

    /*
    Reading a DF:
    - format
    - schema or inferSchema = true
    - path
    - zero or more options
     */
    val carsDF = spark.read
      .format("json")
      .schema(carsSchema) // enforce a schema
      .option("mode", "failFast") // dropMalformed, permissive (default)
      .option("path", "src/main/resources/data/cars.json")
      .load()

    println("AFTER READING CARS DF")
    carsDF.show()

    // alternative reading with options map
    val carsDFWithOptionMap = spark.read
      .format("json")
      .options(
        Map(
          "mode" -> "failFast",
          "path" -> "src/main/resources/data/cars.json",
          "inferSchema" -> "true"
        )
      )
      .load()

    println("AFTER READING CARS DF WITH OPTIONS MAP")
    carsDFWithOptionMap.show()

    /*
   Writing DFs
   - format
   - save mode = overwrite, append, ignore, errorIfExists
   - path
   - zero or more options
     */
    carsDF.write
      .format("json")
      .mode(SaveMode.Overwrite)
      .save("src/main/resources/data/cars_dupe.json")

    println("AFTER WRITING CARS DF")

    // JSON flags
    val carDfWithJsonFlag = spark.read
      .schema(carsSchema)
      .option(
        "dateFormat",
        "yyyy-MM-dd"
      ) // couple with schema; if Spark fails parsing, it will put null
      .option("allowSingleQuotes", "true")
      .option(
        "compression",
        "uncompressed"
      ) // bzip2, gzip, lz4, snappy, deflate
      .json("src/main/resources/data/cars.json")

    println("AFTER READING CARS DF WITH JSON FLAGS")
    carDfWithJsonFlag.show()

    // CSV flags
    val stocksSchema = StructType(
      Array(
        StructField("symbol", StringType),
        StructField("date", DateType),
        StructField("price", DoubleType)
      )
    )

    spark.read
      .schema(stocksSchema)
      .option("dateFormat", "MMM d yyyy")
      .option("header", "true")
      .option("sep", ",")
      .option("nullValue", "")
      .csv("src/main/resources/data/stocks.csv")

    // Parquet
    carsDF.write
      .mode(SaveMode.Overwrite)
      .save("src/main/resources/data/cars.parquet")

    // Text files
    spark.read.text("src/main/resources/data/sampleTextFile.txt").show()

    // Reading from a remote DB
    val driver = "org.postgresql.Driver"
    val url = "jdbc:postgresql://localhost:5432/rtjvm"
    val user = "docker"
    val password = "docker"

//    val employeesDF = spark.read
//      .format("jdbc")
//      .option("driver", driver)
//      .option("url", url)
//      .option("user", user)
//      .option("password", password)
//      .option("dbtable", "public.employees")
//      .load()
//
//    println("\n\n\n\n\t\t\tEmployees DF:\t\t\t\n\n\n\n")
//    employeesDF.show(999)

    val moviesSchema = StructType(
      Array(
        StructField("Title", StringType),
        StructField("US_Gross", IntegerType),
        StructField("Worldwide_Gross", IntegerType),
        StructField("US_DVD_Sales", IntegerType, nullable = true),
        StructField("Production_Budget", IntegerType),
        StructField("Release_Date", StringType),
        StructField("MPAA_Rating", StringType),
        StructField("Running_Time_min", IntegerType, nullable = true),
        StructField("Distributor", StringType),
        StructField("Source", StringType, nullable = true),
        StructField("Major_Genre", StringType, nullable = true),
        StructField("Creative_Type", StringType, nullable = true),
        StructField("Director", StringType, nullable = true),
        StructField("Rotten_Tomatoes_Rating", DoubleType, nullable = true),
        StructField("IMDB_Rating", DoubleType),
        StructField("IMDB_Votes", IntegerType)
      )
    )

    val movie = spark.read
      .format("json")
      .schema(moviesSchema)
      .option("mode", "failFast")
      .json("src/main/resources/data/movies.json")

    movie.show()

    movie.write
      .format("csv")
      .mode(SaveMode.Overwrite)
      .option("header", "true")
      .option("sep", "\t")
      .save("src/main/resources/data/movies2.csv")

    movie.write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .save("src/main/resources/data/movies2.parquet")

    movie.write
      .format("jdbc")
      .option("driver", driver)
      .option("url", url)
      .option("user", user)
      .option("password", password)
      .option("dbtable", "public.movies")
      .save()

    /** Exercise: read the movies DF, then write it as
      * - tab-separated values file
      * - snappy Parquet
      * - table "public.movies" in the Postgres DB
      */

    val moviesDF = spark.read.json("src/main/resources/data/movies.json")

    // TSV
    moviesDF.write
      .format("csv")
      .option("header", "true")
      .option("sep", "\t")
      .save("src/main/resources/data/movies.csv")

    // Parquet
    moviesDF.write.save("src/main/resources/data/movies.parquet")

    // save to DF
//    moviesDF.write
//      .format("jdbc")
//      .option("driver", driver)
//      .option("url", url)
//      .option("user", user)
//      .option("password", password)
//      .option("dbtable", "public.movies")
//      .save()
  }
}
