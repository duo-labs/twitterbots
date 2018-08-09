name := "twitter-bots"

version := "1.0"

scalaVersion := "2.11.11"

mainClass in assembly := Some("com.duo.twitterbots.ExtractionApp")
assemblyJarName in assembly := "twitter-bots.jar"

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

// Spark
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.2.0",
  "org.apache.spark" %% "spark-sql" % "2.2.0",
  "org.apache.spark" %% "spark-hive" % "2.2.0"
)


// AWS dependencies if pulling data from S3
libraryDependencies ++= Seq(
  "com.amazonaws" % "aws-java-sdk-core" % "1.11.84",
  "com.amazonaws" % "aws-java-sdk-s3" % "1.11.84",
  "org.apache.hadoop" % "hadoop-aws" % "2.8.0"
)

// TOML
libraryDependencies += "com.moandjiezana.toml" % "toml4j" % "0.7.1"

// CLI parsing
libraryDependencies += "org.rogach" %% "scallop" % "3.1.0"

// Distance calculations
libraryDependencies += "com.javadocmd" % "simplelatlng" % "1.3.1"