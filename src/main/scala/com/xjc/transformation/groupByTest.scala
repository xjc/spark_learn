package com.xjc.transformation

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._

/**
def groupBy[K](f: (T) ⇒ K, numPartitions: Int)(implicit kt: ClassTag[K]): RDD[(K, Iterable[T])]
Return an RDD of grouped elements.
 */

object groupByTest {
    def main(args:Array[String]) {
        val conf = new SparkConf().setAppName("groupByTest").
            setMaster("local[2]")
            val sc = new SparkContext(conf)
            val data = 1 to 10
            val rdd = sc.parallelize(data, 2)    

            rdd.groupBy(_ + 1).collect.foreach(println)
            /**
            res29: Array[(Int, Iterable[Int])] = Array((4,CompactBuffer(3)), (6,CompactBuffer(5)), (8,CompactBuffer(7)), (10,CompactBuffer(9)), (2,CompactBuffer(1)), (11,
            CompactBuffer(10)), (3,CompactBuffer(2)), (7,CompactBuffer(6)), (9,CompactBuffer(8)), (5,CompactBuffer(4)))
            */
            
            rdd.groupBy(i=>i % 2).collect.foreach(println)
            /**
            (0,CompactBuffer(2, 4, 6, 8, 10))
            (1,CompactBuffer(1, 3, 5, 7, 9))
            */

            sc.stop
    }
}
