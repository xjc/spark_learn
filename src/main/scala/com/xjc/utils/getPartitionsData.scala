package com.xjc.utils

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object getPartitionsDataTest {

    def printParSet[T](a:RDD[T]) {
        import scala.collection.mutable.Map
            val partCheckRdd = a.mapPartitionsWithIndex((i,j) => {
            var map1 = Map[String, List[T]]()
            val index_name = "index_" + i.toString
            map1.put(index_name, List())
            /**
            for (x <- j) {
                if (map1.contains(index_name)) {
                    map1(index_name) = x::map1.get(index_name).get
                } else {
                    map1.put(index_name, List(x))
                }
            }
            */
            for (x <- j) {
                map1(index_name) = x::map1.get(index_name).get
            }
            map1.iterator
            }
            )
            partCheckRdd.sortBy(_._1).foreach(println)

    } 
}

