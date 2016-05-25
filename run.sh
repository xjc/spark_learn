spark-submit \
  --class  com.xjc.Orders    \
  --master yarn-client \
  --executor-memory 4G \
  ./target/scala-2.10/spark_test_2.10-0.1-SNAPSHOT.jar

