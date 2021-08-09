name := "RecommendationEngine"

version := "1.0"

scalaVersion := "2.11.8"

//if you have spark library locally, load spark library from local
//unmanagedBase := file("$[Local_Spark_Path]/jars")
//e.g. unmanagedBase := file("/Users/lynnjiang/Downloads/spark-2.1.0-bin-hadoop2.7/jars")

//load spark library and other libraries online
libraryDependencies ++= {
	val sparkVersion = "2.1.0"

	Seq(
		"org.apache.spark" %% "spark-core" % sparkVersion,
		"org.apache.spark" %% "spark-mllib" % sparkVersion,
		"org.apache.spark" %% "spark-sql" % sparkVersion
	)
}

//Scopt
libraryDependencies += "com.github.scopt" %% "scopt" % "3.3.0"
//joda-time
libraryDependencies += "joda-time" % "joda-time" % "2.9.3"
//spark-avro
libraryDependencies += "com.databricks" %% "spark-avro" % "3.2.0"

resolvers ++= Seq(
  "Apache HBase" at "https://repository.apache.org/content/repositories/releases",
  "scala-tools" at "https://oss.sonatype.org/content/groups/scala-tools",
  Resolver.sonatypeRepo("public")
)