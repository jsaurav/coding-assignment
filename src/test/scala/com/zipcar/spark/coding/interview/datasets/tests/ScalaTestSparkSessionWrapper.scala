package com.zipcar.spark.coding.interview.datasets.tests

import org.apache.spark.sql.SparkSession

trait ScalaTestSparkSessionWrapper {
  
  lazy val spark: SparkSession =  { 
                                    SparkSession
                                    .builder()
                                    .master("local")
                                    .appName("Zipcar Coding Interview")
                                    .getOrCreate()
                                  }
  
}