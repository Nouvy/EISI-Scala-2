ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "3.3.5"
val sparkVersion = "3.3.2"

lazy val root = (project in file("."))
  .settings(
    name := "transactions_massives_scala",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % sparkVersion cross CrossVersion.for3Use2_13,
      "org.apache.spark" %% "spark-sql" % sparkVersion cross CrossVersion.for3Use2_13
    )
  )
