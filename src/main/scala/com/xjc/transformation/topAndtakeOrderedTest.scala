package com.xjc.transformation

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import com.xjc.utils.getPartitionsDataTest._

/**
 * def sortBy[K](f: (T) ⇒ K, ascending: Boolean = true, numPartitions: Int = this.partitions.length)(implicit ord: Ordering[K], ctag: ClassTag[K]): RDD[T]
 * Return this RDD sorted by the given key function.
 */

object topAndtakeOrderedTest {
  def main(args:Array[String]) {
    val conf = new SparkConf().setAppName("topAndtakeOrderedTest").
    setMaster("local[2]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val data = 1 to 20
    val rdd1 = sc.parallelize(data, 3)    
    rdd1.takeOrdered(3).foreach(i=>print(i + " " ))
    //1 2 3 
    rdd1.top(3).foreach(i=>print(i + " " ))
    //20 19 18 

    sc.stop
  }
}

