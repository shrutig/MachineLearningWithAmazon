name := "aws-machine-learning-project"

version in ThisBuild := "1.0"

scalaVersion := "2.11.6"

libraryDependencies ++= Seq("com.amazonaws" % "aws-java-sdk" % "1.9.30",
  "com.typesafe" % "config" % "1.3.0",
  "com.zaxxer" % "HikariCP" % "2.4.1",
  "org.apache.hive" % "hive" % "1.2.1",
  "io.ddf" % "ddf_core_2.10" % "1.4.0-SNAPSHOT",
  "io.ddf" % "ddf_jdbc_2.10" % "1.4.0-SNAPSHOT",
  "io.ddf" % "ddf_spark_2.10" % "1.4.0-SNAPSHOT")

resolvers += Resolver.mavenLocal

