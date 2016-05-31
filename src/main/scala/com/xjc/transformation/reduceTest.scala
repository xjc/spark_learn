package com.xjc.transformation

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import com.xjc.utils.getPartitionsDataTest._

/**
 def reduce(f: (T, T) ⇒ T): T
 Reduces the elements of this RDD using the specified commutative and associative binary operator.
 */

object  reduceTest {
  def main(args:Array[String]) {
    val conf = new SparkConf().setAppName("reduceTest").
    setMaster("local[2]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val data = 1 to 20
    val rdd = sc.parallelize(data, 3)    
    rdd.reduce(_ + _)
    val a = rdd.reduce((x,y) => {if(x>=y) x else y})
    println(a)
    val b = rdd.reduce((x,y) => {if(x>=y) y else x})
    println(b)
    val c = rdd.map(i=>Set(i)).reduce(_ ++ _)
    println(c)

    sc.stop
  }
}

/**
 * ##################################################
 * 3
 * (index_1,List(9))
 * (index_0,List(5))
 * (index_2,List())
 * ##################################################
 * 3
 * (index_0,List())
 * (index_1,List(13, 11, 10, 8))
 * (index_2,List(20, 16))
 * ##################################################
 * 3
 * (index_0,List())
 * (index_1,List(12))
 * (index_2,List(18))
 * ##################################################
 * 3
 * (index_0,List(6, 4, 3, 2, 1))
 * (index_1,List(7))
 * (index_2,List(19, 17, 15, 14))
 */
