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

            //car
            //def cartesian[U](other: RDD[U])(implicit arg0: ClassTag[U]): RDD[((K, C), U)]
            val cartesianRdd = rdd1.cartesian(rdd2)
            cartesianRdd.collect.foreach(println)
            // [10 * 10

            sc.stop
    }
}

/**
  (1,7)
  (1,8)
  (1,9)
  (1,10)
  (1,11)
  (2,7)
  (2,8)
  (2,9)
  (2,10)
  (2,11)
  (3,7)
  (3,8)
  (3,9)
  (3,10)
  (3,11)
  (4,7)
 */
