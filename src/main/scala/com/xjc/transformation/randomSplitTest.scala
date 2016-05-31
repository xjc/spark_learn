package com.xjc.transformation

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import com.xjc.utils.getPartitionsDataTest._

/**
def randomSplit(weights: Array[Double], seed: Long = Utils.random.nextLong): Array[RDD[T]]
Randomly splits this RDD with the provided weights.
weights
weights for splits, will be normalized if they don't sum to 1
seed
random seed
returns
split RDDs in an array
 */

object  randomSplitTest {
    def main(args:Array[String]) {
        val conf = new SparkConf().setAppName("randomSplitTest").
            setMaster("local[2]")
            val sc = new SparkContext(conf)
            sc.setLogLevel("ERROR")
            val data = 1 to 20
            val rdd = sc.parallelize(data, 3)    

            var rddArray1 = rdd.randomSplit(Array(0.1,0.2,0.3,0.4))
            for ( i<- rddArray1 )  {
                println("#"*50)
                println(i.partitions.size)
                printParSet(i)
            }


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
