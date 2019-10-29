package com.zipcar.spark.coding.interview.datasets.tests

import org.scalatest.FunSuite
import org.scalatest.Matchers
import org.scalatest.BeforeAndAfterEach
import com.zipcar.spark.coding.interview.datasets.Weather

class TestWeatherProblem2 extends FunSuite with ScalaTestSparkSessionWrapper {
  
  import spark.implicits._
  
  test("Validating the size of a dataset by asserting on the count.") {
      val actualWeatherDataSet =  spark.read.format("json").load("src/test/resources/weather_stations_subset.json").as[Weather]
      
      val actualCount = actualWeatherDataSet.count()
      
      assert(actualCount == 6)
  }
}