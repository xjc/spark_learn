#sbt clean compile

spark-submit \
  --class  com.xjc.viscosityCust \
  --master yarn-client \
  --executor-memory 12G \
  ./target/scala-2.10/spark_test_2.10-0.1-SNAPSHOT.jar


