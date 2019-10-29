package com.zipcar.spark.coding.interview.datasets

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession


/**
 * Read the data into a Dataset. 
 *	 1. Print the Dataset.
 *  2. Save the Dataset to a CSV file.
 */

object WeatherProblem1 {
  
  val JSON_FORMAT = "json"
  
  def main(args:Array[String]) = {
    
    if(args.length < 2) {
      println("Usage:\n\n\t bin/spark-submit.cmd --class " + WeatherProblem1.getClass.getName + " <jar_file_name> <inputFilePath> <outputFilePath>")
      System.exit(1)
    }
    
    val inputFilePath = args(0)
    val outputFilePath = args(1)
    
    // Create configuration object
    val sparkConf = new SparkConf().setAppName("Weather-Application").setMaster("local[*]")
    
    // Create spark session object
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    
    // Creating raw dataframe by loading the source data.
    val weather_raw_data_df = spark.read.format(JSON_FORMAT).load(inputFilePath)
    
    import spark.implicits._
    
    // Create weathers dataset
    val weather_ds = weather_raw_data_df.as[Weather]
    
    // Caching the dataset since we will be callign multiple actions on it.
    weather_ds.cache()
    
    // Print all the data.
    weather_ds.foreach(data => println(data))
    
    // Save it to a CSV file.
    weather_ds.write.option("header","true").csv(outputFilePath)
    
    weather_ds.unpersist()
    
    spark.stop()
    
  }
  
}