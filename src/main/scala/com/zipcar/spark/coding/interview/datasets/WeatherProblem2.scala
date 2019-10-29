package com.zipcar.spark.coding.interview.datasets

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Dataset

/**
 * Count the number of records and print it to the console.
 */
object WeatherProblem2 {
  
  val JSON_FORMAT = "json"
  
  def main(args:Array[String]) = {
    
    if(args.length < 1) {
      println("Usage:\n\n\t bin/spark-submit.cmd --class " + WeatherProblem2.getClass.getName + " <jar_file_name> <inputFilePath>")
      System.exit(1)
    }
    
    val inputFilePath = args(0)
    
    // Create configuration object
    val sparkConf = new SparkConf().setAppName("Weather-Application").setMaster("local[*]")
    
    // Create spark session object
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    
    // Creating raw dataframe by loading the source data.
    val weather_raw_data_df = spark.read.format(JSON_FORMAT).load(inputFilePath)
    
    import spark.implicits._
    
    // Create weathers dataset
    val weather_ds = weather_raw_data_df.as[Weather]
    
    // Print all the data.
    val count = weather_ds.count() 
    
    println(s"Total number of records =   $count")
    
    spark.stop()
    
  } 
}