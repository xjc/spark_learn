package com.xjc.transformation

import com.xjc.utils.getPartitionsDataTest._
import org.apache.spark.SparkContext._
import org.apache.spark.{SparkConf, SparkContext}

/**
 * def foldByKey(zeroValue: V)(func: (V, V) ⇒ V): RDD[(K, V)]
 * Merge the values for each key using an associative function and a neutral "zero value" which may be added to the result an arbitrary number of times, and must not change the result (e.
 */

object foldByKeyTest {
  def main(args:Array[String]) {
    val conf = new SparkConf().setAppName("foldByKeyTest").
    setMaster("local[2]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val rdd1 = sc.parallelize(Seq("a"->1, "b"->2, "c"->3, "d"->4, "e"->5, "a"->2, "a"->3), 4)
    rdd1.countByKey.foreach(i=>print(i._1 + ":" + i._2 + "  ")) 
    rdd1.foldByKey(0)((a,b)=>a+b).sortBy(_._1).collect.foreach(println)
    /**
     * (a,6)
     * (b,2)
     * (c,3)
     * (d,4)
     * (e,5)
     */
    rdd1.foldByKey(1)((a,b)=>a+b).sortBy(_._1).collect.foreach(println)
    /**
     * (a,8)
     * (b,3)
     * (c,4)
     * (d,5)
     * (e,6)
     */

    sc.stop
  }
}

