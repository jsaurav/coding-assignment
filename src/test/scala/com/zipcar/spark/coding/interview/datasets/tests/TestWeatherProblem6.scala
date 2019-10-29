package com.zipcar.spark.coding.interview.datasets.tests

import org.scalatest.FunSuite

import com.zipcar.spark.coding.interview.datasets.Weather
import org.apache.spark.sql.Dataset

class TestWeatherProblem6 extends FunSuite with ScalaTestSparkSessionWrapper {
  
  import spark.implicits._
  
  spark.conf.set("spark.sql.shuffle.partitions", 4)

  test("Validating the count of stations by province.") {
      val actualWeatherDataSet =  spark.read.format("json").load("src/test/resources/weather_stations_subset.json").as[Weather]
      
      val actualWeatherDataSetGrouped = actualWeatherDataSet.select("stationName","province").groupBy("province").count().orderBy("province")
      
      val expectedGroupCount = Map("BC" -> 6)
      
      val rows = actualWeatherDataSetGrouped.collect()
      
      rows.foreach(row => assert(row.getLong(1) == expectedGroupCount.get(row.getString(0)).get))   
  }
 
}