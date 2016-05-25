package com.xjc.transformation

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import java.sql.{Timestamp, DriverManager, PreparedStatement, Connection}

object sampleTest {
      def main(args:Array[String]) {
              val conf = new SparkConf().setAppName("MapPartitionsTest").setMaster("local[2]")
              val sc = new SparkContext(conf)
              val data = 1 to 10
              val rdd = sc.parallelize(data, 2)
              //map test
              rdd.sample(true, .2).collect.foreach(println)
              rdd.sample(true, .1).collect.foreach(println)
              rdd.sample(true, .3).collect.foreach(println)
              rdd.sample(false, .2).collect.foreach(println)

              sc.stop
  }
}
