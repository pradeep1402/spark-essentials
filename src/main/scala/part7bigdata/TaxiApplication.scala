package part7bigdata

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object TaxiApplication {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .config("spark.master", "local")
      .appName("Taxi Big Data Application")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    import spark.implicits._

    val taxiDF =
      spark.read.load("src/main/resources/data/yellow_taxi_jan_25_2018")
    taxiDF.printSchema()

    val taxiZonesDF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("src/main/resources/data/taxi_zones.csv")
    taxiZonesDF.printSchema()

    taxiZonesDF.show(false)
    taxiDF.show(false)

    /** Questions:
      * 5. What are the top 3 pickup/dropoff zones for long/short trips?
      * 6. How are people paying for the ride, on long/short trips?
      * 7. How is the payment type evolving with time?
      * 8. Can we explore a ride-sharing opportunity by grouping close short trips?
      */

    // 1. Which zones have the most pickups/drop-offs overall?
    val pickups = taxiDF
      .groupBy("PULocationID")
      .count()
      .join(taxiZonesDF, col("PULocationID") === col("LocationID"))
      .groupBy("Borough")
      .agg(sum(col("count")))
      .orderBy($"sum(count)".desc_nulls_last)

    pickups.show()

    val dropOff = taxiDF
      .groupBy("DOLocationID")
      .count()
      .join(taxiZonesDF, col("DOLocationID") === col("LocationID"))
      .groupBy("Borough")
      .agg(sum($"count"))
      .orderBy($"sum(count)".desc_nulls_last)

    dropOff.show()

    // 2. What are the peak hours for taxi?
    taxiDF
      .withColumn("hours", hour(col("tpep_pickup_datetime")))
      .groupBy("hours")
      .agg(count("*").alias("total_rides"))
      .orderBy(col("total_rides").desc_nulls_last)
      .select(col("total_rides"), col("hours"))
      .show()

    // 3. How are the trips distributed by length? Why are people taking the cab?
    taxiDF
      .select(
        lit(min(col("trip_distance"))).as("min"),
        lit(max(col("trip_distance"))).as("max"),
        lit(stddev(col("trip_distance"))).as("stddev"),
        lit(mean(col("trip_distance")))
      )
      .show()

    val longDistanceThreshold = 30
    val tripsWithLengthDF =
      taxiDF.withColumn("isLong", col("trip_distance") >= longDistanceThreshold)

    tripsWithLengthDF.show()

    // 4. What are the peak hours for long/short trips?
    tripsWithLengthDF
      .groupBy(hour(col("tpep_pickup_datetime")))
      .count()
      .sort($"count".desc)
      .show()

  }
}
