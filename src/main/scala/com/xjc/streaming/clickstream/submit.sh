spark-submit --class org.apache.spark.examples.streaming.clickstream.PageViewGenerator \
--master local  \
/home/xjc/scripts/sbt_test/spark_test/target/scala-2.11/spark_test_2.11-0.1-SNAPSHOT.jar  \
 44444 10


spark-submit --class org.apache.spark.examples.streaming.clickstream.PageViewStream \
--master local  \
/home/xjc/scripts/sbt_test/spark_test/target/scala-2.11/spark_test_2.11-0.1-SNAPSHOT.jar  \
popularUsersSeen localhost 44444

slidingPageCounts |  errorRatePerZipCode |   activeUserCount | popularUsersSeen

