val spark = "org.apache.spark" %% "spark-core" % "1.6.0"
val spark_streaming = "org.apache.spark" %% "spark-streaming" % "1.6.0"
val spark_sql = "org.apache.spark" %% "spark-sql" % "1.6.0"
val spark_hive = "org.apache.spark" %% "spark-hive" % "1.6.0"
val sparkStreamKafka = "org.apache.spark" % "spark-streaming-kafka_2.11" % "1.6.0"
val mysql_driver = "mysql" % "mysql-connector-java" % "5.1.38"

lazy val commonSettings = Seq(
  organization:="com.xjc",
  name:="streaming_api_log",
  scalaVersion:="2.11.7"
)

lazy val root = (project in file(".")).
  settings(commonSettings: _*).
  settings(
    libraryDependencies += spark,
    libraryDependencies += spark_streaming,
    libraryDependencies += spark_sql,
    libraryDependencies += spark_hive,
    libraryDependencies += sparkStreamKafka,
    libraryDependencies += mysql_driver,
    libraryDependencies += "org.anarres.lzo" % "lzo-hadoop" % "1.0.0",
    resolvers += "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots",
    resolvers += "Local Maven Repository" at "file:///home/xjc/.m2/repository"
  )
