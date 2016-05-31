package com.xjc.transformation

import com.xjc.utils.getPartitionsDataTest._
import org.apache.spark.SparkContext._
import org.apache.spark.{SparkConf, SparkContext}

/**
 * def zip[U](other: RDD[U])(implicit arg0: ClassTag[U]): RDD[(T, U)]
 * Zips this RDD with another one, returning key-value pairs with the first element in each RDD, second element in each RDD, etc. Assumes that the two RDDs have the *same number of partitions* and the *same number of elements in each partition* (e.g. one was made through a map on the other).
 *
 * def zipWithIndex(): RDD[(T, Long)] Zips this RDD with its element indices. The ordering is first based on the partition index and then the ordering of items within each partition. So the first item in the first partition gets index 0, and the last item in the last partition receives the largest index.
 *
 * This is similar to Scala's zipWithIndex but it uses Long instead of Int as the index type. This method needs to trigger a spark job when this RDD contains more than one partitions.
 *
 * Note that some RDDs, such as those returned by groupBy(), do not guarantee order of elements in a partition. The index assigned to each element is therefore not guaranteed, and may even change if the RDD is reevaluated. If a fixed ordering is required to guarantee the same index assignments, you should sort the RDD with sortByKey() or save it to a file.
 */

object sortByKeyTest {
  def main(args:Array[String]) {
    val conf = new SparkConf().setAppName("sortByKeyTest").
    setMaster("local[2]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val rdd1 = sc.parallelize(Seq("a"->1, "b"->2, "a1"->3, "d1"->4, "d"->5))
    val rdd1_1 = rdd1.map(i=>(i._2, i._1)).sortByKey(false).map(i=>(i._2,i._1))
    rdd1_1.collect.foreach(i=>println(i + "   " ))

    val rdd1_2 = rdd1.sortBy(_._2,false)
    rdd1_2.collect.foreach(i=>println(i + "   " ))

    sc.stop
  }
}

