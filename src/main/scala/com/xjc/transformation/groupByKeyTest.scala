package com.xjc.transformation

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import java.sql.{Timestamp, DriverManager, PreparedStatement, Connection}

/**
groupByKey([numTasks])

When called on a dataset of (K, V) pairs, returns a dataset of (K, Iterable<V>) pairs. 
Note: If you are grouping in order to perform an aggregation (such as a sum or average) over each key, using reduceByKey or aggregateByKey will yield much better performance. 
Note: By default, the level of parallelism in the output depends on the number of partitions of the parent RDD. You can pass an optional numTasks argument to set a different number of tasks.
*/

object groupByKeyTest {
      def main(args:Array[String]) {
              val conf = new SparkConf().setAppName("MapPartitionsTest").setMaster("local[2]")
              val sc = new SparkContext(conf)
              val data = 1 to 10
              val rdd = sc.parallelize(data, 2)
              val rdd1 = rdd.map(_ * 2).union(rdd).map((i) =>(i.toString, 1))
              rdd1.groupByKey.collect.foreach(println)

              rdd1.groupByKey(2).collect.foreach(println)
            
              val file = "file:///home/xjc/abc.txt"
              val fileRdd = sc.textFile(file)
              val rdd3 = fileRdd.flatMap(_.split("\\s")).map(i=>(i,1)).groupByKey
              rdd3.groupByKey(2).collect.foreach(println)
              
              sc.stop
  }
}

/**
(4,CompactBuffer(1, 1))
(8,CompactBuffer(1, 1))
(14,CompactBuffer(1))
(20,CompactBuffer(1))
(7,CompactBuffer(1))
(5,CompactBuffer(1))
(18,CompactBuffer(1))
(6,CompactBuffer(1, 1))
(2,CompactBuffer(1, 1))
(16,CompactBuffer(1))
(9,CompactBuffer(1))
(3,CompactBuffer(1))
(12,CompactBuffer(1))
(1,CompactBuffer(1))
(10,CompactBuffer(1, 1))

*/
