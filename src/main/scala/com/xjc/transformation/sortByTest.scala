package com.xjc.transformation

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import com.xjc.utils.getPartitionsDataTest._

/**
 * def sortBy[K](f: (T) ⇒ K, ascending: Boolean = true, numPartitions: Int = this.partitions.length)(implicit ord: Ordering[K], ctag: ClassTag[K]): RDD[T]
 * Return this RDD sorted by the given key function.
 */

object  sortByTest {
  def main(args:Array[String]) {
    val conf = new SparkConf().setAppName("sortByTest").
    setMaster("local[2]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val data = 1 to 20
    val rdd1 = sc.parallelize(data, 3)    
    val rdd1_1 = rdd1.sortBy(i=>i)
    rdd1.sortBy(i=>0-i).collect.foreach(println)
    rdd1_1.collect.foreach(i=>print (i + " "))
    //1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20
    printParSet(rdd1_1)
    /**
     * (index_1,List(14, 13, 12, 11, 10, 9, 8))
     * (index_0,List(7, 6, 5, 4, 3, 2, 1))
     * (index_2,List(20, 19, 18, 17, 16, 15))
    */


    val rdd2 = sc.parallelize(List("a"->1, "b"->2, "a1"->3, "z"->4, "d"->5), 3)
    val rdd2_1 = rdd2.sortBy(_._2)
    rdd2_1.foreach(println)
    /**
     * (a1,3)
     * (a,1)
     * (z,4)
     * (b,2)
     * (d,5)
     */
    printParSet(rdd2_1)
    /**
     * (index_1,List((z,4), (a1,3)))
     * (index_0,List((b,2), (a,1)))
     * (index_2,List((d,5)))
     */
    rdd2_1.collect.foreach(println)
    /**
     * (a,1)
     * (b,2)
     * (a1,3)
     * (z,4)
     * (d,5)
     */
    
    val rdd2_2 = rdd2.sortBy(_._2, false, 1)
    rdd2_2.foreach(println)

    sc.stop
  }
}

