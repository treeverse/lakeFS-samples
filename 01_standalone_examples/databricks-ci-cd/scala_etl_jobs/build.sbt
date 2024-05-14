ThisBuild / scalaVersion := "2.12.18"
ThisBuild / organization := "com.example"

libraryDependencies += "org.scala-lang" % "scala-library" % "2.12.18"

val sparkVersion = "3.5.0"
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-hive" % sparkVersion % "provided"
)

lazy val transform = (project in file("."))
  .settings(
    name := "etl_jobs"
  )
