name := "hellospark"

version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq (
  "org.apache.spark" %% "spark-core" % "1.6.1",
  "org.apache.spark" %% "spark-mllib" % "1.6.1",
  "com.databricks" %% "spark-csv" % "1.4.0"
)







resolvers ++= Seq(
  "Apache HBase" at "https://repository.apache.org/content/repositories/releases",
  "Typesafe repository" at "http://repo.typesafe.com/typesafe/releases/"
)

fork := true

    