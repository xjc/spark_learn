package com.xjc.transformation

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import java.sql.{Timestamp, DriverManager, PreparedStatement, Connection}

object MapPartitionsTest2 {
      def main(args:Array[String]) {
              val conf = new SparkConf().setAppName("MapPartitionsTest").setMaster("local[8]")
              val sc = new SparkContext(conf)
              val data = 1 to 10000
              val rdd = sc.parallelize(data, 8)
              //map test

              implicit def int2str(num:Int) :String = String.valueOf(num)

              rdd.mapPartitions(
                  record=>{ 
                          var list1 = List[String]()
                          while ( record.hasNext) {
                                  val data = record.next
                                  if ( data % 500 == 0 ) list1 = data::list1
                          }
                          list1.iterator
                  }
              ).collect.foreach(println)
              sc.stop
  }
}
