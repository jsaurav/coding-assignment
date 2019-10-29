package com.zipcar.spark.coding.interview.datasets

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 * Get a distinct list of weather station names
 *   	1. Print the Dataset.
 *   	2. Save the Dataset to a CSV file.
 */
object WeatherProblem3 {

  def main(args: Array[String]) = {
    
    if(args.length < 2) {
      println("Usage:\n\n\t bin/spark-submit.cmd --class " + WeatherProblem3.getClass.getName + " <jar_file_name> <inputFilePath> <outputFilePath>")
      System.exit(1)
    }

    val inputFilePath = args(0)

    val outputFilePath = args(1)
    
    // Create configuration object
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Weather-Application")

    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    val weather_data_raw = spark.read.json(inputFilePath)
    
    // to enable encoders
    import spark.implicits._
    
    val weather_ds = weather_data_raw.as[Weather]
    
    // Get distinct weathers.
    
    spark.conf.set("spark.sql.shuffle.partitions", 12)
    
    val weather_station_names = weather_ds.map(weather => weather.stationName)
    
    val weather_station_names_distinct = weather_station_names.distinct()
    weather_station_names_distinct.cache()
    
    weather_station_names_distinct.show(100,false)
    
    weather_station_names_distinct.write.option("header","true").csv(outputFilePath)

    weather_station_names_distinct.unpersist()
    spark.stop()
  }

}