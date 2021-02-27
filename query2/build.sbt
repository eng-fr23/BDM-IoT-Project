name := "Query2"

version := "0.1"

scalaVersion := "2.12.13"

lazy val spark = "org.apache.spark" %% "spark-core" % "3.0.1"
lazy val mongo = "org.mongodb.scala" %% "mongo-scala-driver" % "4.2.0"
//lazy val scallop = "org.rogach" %% "scallop" % "3.5.1"
//lazy val log = "ch.qos.logback" % "logback-classic" % "0.9.28"

libraryDependencies += spark
libraryDependencies += mongo
//libraryDependencies += scallop
//libraryDependencies += log


lazy val root = (project in file("."))
  .settings(
    name := "Query2"
  )