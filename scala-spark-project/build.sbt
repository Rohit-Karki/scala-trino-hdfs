name := "IcebergSparkScala"

version := "0.1"

scalaVersion := "2.12.15"

libraryDependencies ++= Seq(
  "org.apache.iceberg" %% "iceberg-spark-runtime-3.3" % "1.5.2",
  "org.postgresql" % "postgresql" % "42.6.0",
  "org.apache.hadoop" % "hadoop-aws" % "3.3.4",
  "com.amazonaws" % "aws-java-sdk-bundle" % "1.12.262"
)

ThisBuild / resolvers += "Apache Releases" at "https://repository.apache.org/content/repositories/releases/"
