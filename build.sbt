val spark = "org.apache.spark" %% "spark-core" % "1.6.0"
val spark_streaming = "org.apache.spark" %% "spark-streaming" % "1.6.0"
val spark_sql = "org.apache.spark" %% "spark-sql" % "1.6.0"
val sparkStreamKafka = "org.apache.spark" %% "spark-streaming-kafka" % "1.6.0"
val sparkHive = "org.apache.spark" %% "spark-hive" % "1.6.0"
val hadoop_common = "org.apache.hadoop" % "hadoop-common" % "2.2.0"


lazy val commonSettings = Seq(
        organization:="com.xjc",
        name:="spark_test",
        scalaVersion:="2.10.4"
)

lazy val root = (project in file(".")).
    settings(commonSettings: _*).
    settings(
        libraryDependencies += spark,
        libraryDependencies += spark_streaming,
        libraryDependencies += spark_sql,
        libraryDependencies += sparkStreamKafka,
        libraryDependencies += sparkHive,
        //libraryDependencies += hadoop_common,
        resolvers += "Local Maven Repository" at "file:///data3/maven/M2Repository/"
    )  
