package com.xjc.transformation

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import com.xjc.utils.getPartitionsDataTest._

/**
sample(withReplacement: Boolean, fraction: Double, seed: Long = Utils.random.nextLong): RDD[T]
Return a sampled subset of this RDD.
withReplacement
can elements be sampled multiple times (replaced when sampled out)
fraction
expected size of the sample as a fraction of this RDD's size without replacement: probability that each element is chosen; fraction must be [0, 1] with replacement: expected number of times each element is chosen; fraction must be >= 0
seed
seed for the random number generator
 */

object sampleTest {
    def main(args:Array[String]) {
        val conf = new SparkConf().setAppName("sampleTest").
            setMaster("local[2]")
            val sc = new SparkContext(conf)
            val data = 1 to 20
            val rdd = sc.parallelize(data, 3)    

            var rdd1 = rdd.sample(true, 0.1)
            println(rdd1.partitions.size)
            //3
            printParSet(rdd1)
            /**
            (index_1,List(12))
            (index_0,List(5, 3))
            (index_2,List())
            */
            //rdd1.collect.foreach(println)
            var rdd2 = rdd.sample(true, 0.5)
            println(rdd2.partitions.size)
            printParSet(rdd2)
            /**
            (index_1,List(13, 11, 8, 8))
            (index_0,List(5, 5, 3))
            (index_2,List(20, 20, 19, 18, 16, 15, 15))
            */


            sc.stop
    }
}
