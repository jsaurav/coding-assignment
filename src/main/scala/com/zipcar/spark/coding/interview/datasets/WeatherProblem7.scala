package com.zipcar.spark.coding.interview.datasets

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import org.apache.spark.sql.functions.{col, desc}
import org.apache.spark.sql.types.FloatType

/**
 *
 * Find the furthest north / south / west / east weather station
 *   1. Print the Dataset.
 *   2. Save the Dataset to a CSV file.
 *
 * To solve this problem we need to have understanding of latitudes and
 * longitudes without understanding then we won't we able to solve them.
 * 
 * Latitude - The latitude of the earth is a coordinate that is north or
 * south of the equator.It tells about the how far it is north or south from
 * the equator. For north pole it is 90 degree and south pole it is -90 degree.
 * 
 * Longitude - The longitude of the earth is a coordinate east or west of an
 * imaginary line called the Prime Meridian. The farthest point away from the 
 * Prime Meridian is +180 degree eastward and -180 degree westward.
 *
 */

object WeatherProblem7 {

  def main(args: Array[String]) = {

    if (args.length < 2) {
      println("Usage:\n\n\t bin/spark-submit.cmd --class " + WeatherProblem7.getClass.getName + " <jar_file_name> <inputFilePath> <outputFilePath>")
      System.exit(1)
    }

    val inputFilePath = args(0)

    val outputFilePath = args(1)

    // Create configuration object
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Weather-Application")

    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    val weather_data_raw = spark.read.json(inputFilePath)

    import spark.implicits._

    val weather_ds = weather_data_raw.as[Weather]

    // Get distinct weathers.

    spark.conf.set("spark.sql.shuffle.partitions", 12)
    
    val weather_ds_t1 = weather_ds.select(col("stationName"), col("lat").cast(FloatType), col("lon").cast(FloatType))
    
    // Caching this dataset since we will be using it multiple times for
    // getting furthest stations.
    
    weather_ds_t1.cache()
    
    // Furthest north station
    val weather_ds_furthest_north = weather_ds.select("stationName", "lat").orderBy(desc("lat")).limit(1)
    weather_ds_furthest_north.cache()
    
    
    // Furthest south station and caching.
    val weather_ds_furthest_south = weather_ds.select("stationName", "lon").orderBy("lat").limit(1)
    weather_ds_furthest_south.cache()
    
    // Furthest east station and caching.
    val weather_ds_furthest_east = weather_ds.select("stationName", "lon").orderBy(desc("lon")).limit(1)
    weather_ds_furthest_east.cache()
    
    
    // Furthest south station and caching.
    val weather_ds_furthest_west = weather_ds.select("stationName", "lon").orderBy("lon").limit(1)
    weather_ds_furthest_west.cache()
    
    weather_ds_furthest_north.show()
    weather_ds_furthest_south.show()
    weather_ds_furthest_east.show()
    weather_ds_furthest_west.show()
    
    weather_ds_furthest_north.write.option("header", "true").csv(outputFilePath + "/NORTH")
    weather_ds_furthest_south.write.option("header", "true").csv(outputFilePath + "/SOUTH")
    weather_ds_furthest_east.write.option("header", "true").csv(outputFilePath + "/EAST")
    weather_ds_furthest_west.write.option("header", "true").csv(outputFilePath + "/WEST")
    
    weather_ds_t1.unpersist()
    weather_ds_furthest_north.unpersist()
    weather_ds_furthest_south.unpersist()
    weather_ds_furthest_east.unpersist()
    weather_ds_furthest_west.unpersist()
    
    spark.stop()
  }

}