package com.zipcar.spark.coding.interview.datasets.tests

import org.scalatest.FunSuite
import org.scalatest.Matchers
import org.scalatest.BeforeAndAfterEach
import com.zipcar.spark.coding.interview.datasets.Weather
import org.apache.spark.sql.functions.udf

class TestWeatherProblem4 extends FunSuite with ScalaTestSparkSessionWrapper {
  
  import spark.implicits._
  
  spark.conf.set("spark.sql.shuffle.partitions", 4)
  
  val toUpper = (data:String) => {
    data.toUpperCase()
  }
  
  spark.udf.register("toUpper", toUpper)
  
  test("Validating the upper case for all the station names.") {
      val actualWeatherDataSet =  spark.read.format("json").load("src/test/resources/weather_stations_subset.json").as[Weather]
      
      val actualWeatherDataSetDistinct = actualWeatherDataSet.select("stationName")
      
      val expectedStationNames = Array("CHEMAINUS", "COWICHAN LAKE FORESTRY", "LAKE COWICHAN", "DISCOVERY ISLAND", "CHEMAINUS", "COWICHAN LAKE FORESTRY")
      
      // or use java.util.arrays
      assert(actualWeatherDataSetDistinct.map(data => data.mkString).map(toUpper).collect().deep == expectedStationNames.deep)
  }
  
  //val toUpperUDF = udf[String, String](toUpper)
}