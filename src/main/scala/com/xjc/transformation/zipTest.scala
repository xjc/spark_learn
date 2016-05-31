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

object  zipTest {
  def main(args:Array[String]) {
    val conf = new SparkConf().setAppName("zipTest").
    setMaster("local[2]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val rdd1 = sc.parallelize(1 to 6)
    val rdd2 = sc.parallelize(Seq("a", "b", "c", "d", "e", "f"))
    rdd1.zip(rdd2).collect.foreach(i=>print(i + "  "))


    /**
     * val rdd2 = sc.parallelize(Seq("a", "b", "c", "d", "e", "f", "g"))
     * rdd1.zip(rdd2).collect.foreach(i=>print(i + "  "))
     * org.apache.spark.SparkException: Can only zip RDDs with same number of elements in each partition
     */
    val rdd3 = sc.parallelize(1 to 30, 3)
    val rdd4 = sc.parallelize(40 to 50, 3)
    val rdd3_1 = rdd3.zipPartitions(rdd4)((iter1, iter2) => iter1 ++ iter2)
    printParSet(rdd3_1)

    val rdd5 = sc.parallelize(1 to 20, 3)
    val rdd5_1 = rdd5.zipWithIndex
    printParSet(rdd5_1)

    val rdd5_2 = rdd5.zipWithUniqueId
    printParSet(rdd5_2)

    sc.stop
  }
}

