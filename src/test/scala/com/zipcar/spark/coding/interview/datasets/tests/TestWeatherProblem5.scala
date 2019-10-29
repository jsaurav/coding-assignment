package com.zipcar.spark.coding.interview.datasets.tests

import org.scalatest.FunSuite
import org.scalatest.Matchers
import org.scalatest.BeforeAndAfterEach
import com.zipcar.spark.coding.interview.datasets.Weather
import org.apache.spark.sql.functions.udf

class TestWeatherProblem5 extends FunSuite with ScalaTestSparkSessionWrapper {
  
  import spark.implicits._
  
  spark.conf.set("spark.sql.shuffle.partitions", 4)

  test("Validating the ordering of elements by station names.") {
      val actualWeatherDataSet =  spark.read.format("json").load("src/test/resources/weather_stations_subset.json").as[Weather]
      
      val actualWeatherDataSetDistinct = actualWeatherDataSet.select("stationName").orderBy("stationName")
      
      val expectedStationNames = Array("CHEMAINUS", "CHEMAINUS", "COWICHAN LAKE FORESTRY", "COWICHAN LAKE FORESTRY", "DISCOVERY ISLAND", "LAKE COWICHAN")
      
      // or use java.util.arrays
      assert(actualWeatherDataSetDistinct.map(data => data.mkString).collect().deep == expectedStationNames.deep)
  }
}