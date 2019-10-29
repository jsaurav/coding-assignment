package com.zipcar.spark.coding.interview.datasets.tests

import org.scalatest.FunSuite

import com.zipcar.spark.coding.interview.datasets.Weather
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions.desc

class TestWeatherProblem7 extends FunSuite with ScalaTestSparkSessionWrapper {
  
  import spark.implicits._
  
  spark.conf.set("spark.sql.shuffle.partitions", 4)

  test("Validating the farthest east , west , north and south stations.") {
      val actualWeatherDataSet =  spark.read.format("json").load("src/test/resources/weather_stations_subset-2.json").as[Weather]
      
      val actualWeatherDataSetT1 = actualWeatherDataSet.select("stationName","lat", "lon")
      actualWeatherDataSetT1.cache()
      
      val weather_ds_furthest_north = actualWeatherDataSetT1.select("stationName", "lat").orderBy(desc("lat")).limit(1)
    
      // Furthest south station and caching.
      val weather_ds_furthest_south = actualWeatherDataSetT1.select("stationName", "lon").orderBy("lat").limit(1)
    
      // Furthest east station and caching.
      val weather_ds_furthest_east = actualWeatherDataSetT1.select("stationName", "lon").orderBy(desc("lon")).limit(1)
      
      // Furthest south station and caching.
      val weather_ds_furthest_west = actualWeatherDataSetT1.select("stationName", "lon").orderBy("lon").limit(1)
    
      assert(weather_ds_furthest_north.collect().head.getString(0) == "GALIANO NORTH")
      assert(weather_ds_furthest_south.collect().head.getString(0) == "METCHOSIN")
      assert(weather_ds_furthest_east.collect().head.getString(0) == "COWICHAN LAKE FORESTRY")
      assert(weather_ds_furthest_west.collect().head.getString(0) == "DISCOVERY ISLAND")
      
      actualWeatherDataSetT1.unpersist()
  }
 
}