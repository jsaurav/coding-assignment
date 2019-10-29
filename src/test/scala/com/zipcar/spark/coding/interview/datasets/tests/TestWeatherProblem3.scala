package com.zipcar.spark.coding.interview.datasets.tests

import org.scalatest.FunSuite
import org.scalatest.Matchers
import org.scalatest.BeforeAndAfterEach
import com.zipcar.spark.coding.interview.datasets.Weather

class TestWeatherProblem3 extends FunSuite with ScalaTestSparkSessionWrapper {
  
  import spark.implicits._
  
  spark.conf.set("spark.sql.shuffle.partitions", 4)
  
  test("Validating the distinct station names.") {
      val actualWeatherDataSet =  spark.read.format("json").load("src/test/resources/weather_stations_subset.json").as[Weather]
      
      val actualWeatherDataSetDistinct = actualWeatherDataSet.select("stationName").distinct()
      
      val expectedStationNames = Array("COWICHAN LAKE FORESTRY", "DISCOVERY ISLAND", "CHEMAINUS", "LAKE COWICHAN")
      
      // or use java.util.arrays
      assert(actualWeatherDataSetDistinct.map(data => data.mkString).collect().deep == expectedStationNames.deep)
  }
}