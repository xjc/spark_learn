package com.xjc.transformation

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import java.sql.{Timestamp, DriverManager, PreparedStatement, Connection}

object relationTest {
      def main(args:Array[String]) {
              val conf = new SparkConf().setAppName("relationTest").setMaster("local[2]")
              val sc = new SparkContext(conf)
              val rdd1 = sc.parallelize(1 to 10, 2)
              val rdd2 = rdd1.map(_ + 6)

              val unionRdd = rdd1.union(rdd2)
              unionRdd.sortBy(i=>i,true,1).foreach(println)

              val intersectionRdd = rdd1.intersection(rdd2)
              intersectionRdd.sortBy(i=>i,true,1).foreach(println)

              val distinctRdd = rdd1.union(rdd2).distinct
              distinctRdd.sortBy(i=>i,true,1).foreach(println)
              //map test

              sc.stop
  }
}
