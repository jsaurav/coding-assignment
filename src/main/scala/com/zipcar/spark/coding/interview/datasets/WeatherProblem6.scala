package com.zipcar.spark.coding.interview.datasets

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 *
 *  Count of the weather stations per province
 *   	1. Print the Dataset.
 *   	2. Save the Dataset to a CSV file.
 *
 */

object WeatherProblem6 {

  def main(args: Array[String]) = {

    if (args.length < 2) {
      println("Usage:\n\n\t bin/spark-submit.cmd --class " + WeatherProblem6.getClass.getName + " <jar_file_name> <inputFilePath> <outputFilePath>")
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

    val weather_grouped_by_province = weather_ds.groupBy("province").count().orderBy("province")
    
    // Caching the grouped RDD, since we are calling the multiple actions on it and
    // to avoid re-execution of DAG again.
    weather_grouped_by_province.cache()

    weather_grouped_by_province.show(100, false)

    weather_grouped_by_province.write.option("header", "true").csv(outputFilePath)

    spark.stop()
  }

}