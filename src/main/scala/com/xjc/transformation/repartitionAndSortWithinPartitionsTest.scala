package com.xjc.transformation

import com.xjc.utils.getPartitionsDataTest._
import org.apache.spark.SparkContext._
import org.apache.spark.{SparkConf, SparkContext}

/**
 * def repartitionAndSortWithinPartitions(partitioner)  
 * Repartition the RDD according to the given partitioner and, within each resulting partition, sort records by their keys. This is more efficient than calling repartition and then sorting within each partition because it can push the sorting down into the shuffle machinery.

 def repartitionAndSortWithinPartitions(partitioner: Partitioner): RDD[(K, V)] = 
   self.withScope {
     new ShuffledRDD[K, V, V](self, partitioner).setKeyOrdering(ordering)
   }
   */

  object repartitionAndSortWithinPartitionsTest {
    def main(args:Array[String]) {
      val conf = new SparkConf().setAppName("repartitionAndSortWithinPartitionsTest").
      setMaster("local[2]")
      val sc = new SparkContext(conf)
      sc.setLogLevel("ERROR")

      val rdd1 = sc.parallelize(Seq("a"->1, "b"->2, "c"->3, "d"->4, "e"->5), 4)
      import org.apache.spark.RangePartitioner
      val rangePartitioner = new RangePartitioner(3, rdd1)
      val rdd1_1 = rdd1.repartitionAndSortWithinPartitions(rangePartitioner)
      printParSet(rdd1_1)
      /**
       (index_0,List((b,2), (a,1)))
       (index_1,List((d,4), (c,3)))
       (index_2,List((e,5)))
       */

      import org.apache.spark.HashPartitioner
      val rdd1_2 = rdd1.repartitionAndSortWithinPartitions(new HashPartitioner(4))
      printParSet(rdd1_2)
      /**
       (index_0,List((d,4)))
       (index_1,List((e,5), (a,1)))
       (index_2,List((b,2)))
       (index_3,List((c,3)))
       */



      sc.stop
    }
  }

  /**
   (d,(CompactBuffer(4),CompactBuffer(44)))
   (e,(CompactBuffer(5),CompactBuffer()))
   (a,(CompactBuffer(1),CompactBuffer()))
   (b,(CompactBuffer(2),CompactBuffer(22)))
   (c,(CompactBuffer(3),CompactBuffer(33)))
   */
