val scalaLangVersion = "2.11.11"
val jdkVersion = "1.8"
val sparkVersion = "2.3.4"

						
lazy val root = (project in file(".")).
  settings(
    name := "spark-interview",
    version := "1.0.0",
    scalaVersion := scalaLangVersion,
    javacOptions ++= Seq("-source", jdkVersion, "-target", jdkVersion, "-Xlint"),
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % sparkVersion,
      "org.apache.spark" %% "spark-hive" % sparkVersion,
      "org.apache.spark" %% "spark-sql" % sparkVersion,

      // following dependencies are only needed for unit tests
      "org.scalatest" %% "scalatest" % "3.0.1" % "test"
    )
  )

  
  
