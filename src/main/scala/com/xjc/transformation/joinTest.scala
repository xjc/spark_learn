package com.xjc.transformation

import com.xjc.utils.getPartitionsDataTest._
import org.apache.spark.SparkContext._
import org.apache.spark.{SparkConf, SparkContext}

/**
 * def join[W](other: RDD[(K, W)], partitioner: Partitioner): RDD[(K, (V, W))]
 * Return an RDD containing all pairs of elements with matching keys in this and other. Each pair of elements will be returned as a (k, (v1, v2)) tuple, where (k, v1) is in this and (k, v2) is in other. Uses the given Partitioner to partition the output RDD.
 */

object joinTest {
  def main(args:Array[String]) {
    val conf = new SparkConf().setAppName("joinTest").
    setMaster("local[2]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val rdd1 = sc.parallelize(Seq("a"->1, "b"->2, "c"->3, "d"->4, "e"->5), 4)
    val rdd2 = sc.parallelize(Seq("b"->22, "c"->33, "d"->44), 2)

    val rdd3 = rdd1.join(rdd2)
    rdd3.partitions.foreach(println)
    rdd3.collect.foreach(println)
    /**
     * (d,(4,44))
     * (b,(2,22))
     * (c,(3,33))
     */

    var rdd4 = rdd1.leftOuterJoin(rdd2)
    rdd4.sortByKey().collect.foreach(println)
    /**
     * (a,(1,None))
     * (b,(2,Some(22)))
     * (c,(3,Some(33)))
     * (d,(4,Some(44)))
     * (e,(5,None))
     */

    val rdd5 = rdd1.fullOuterJoin(rdd2)
    rdd5.sortByKey().collect.foreach(println)
    /**
     *(a,(Some(1),None))
     (b,(Some(2),Some(22)))
     (c,(Some(3),Some(33)))
     (d,(Some(4),Some(44)))
     (e,(Some(5),None))
     */

    sc.stop
  }
}

