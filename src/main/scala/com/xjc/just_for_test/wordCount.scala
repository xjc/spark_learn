package com.xjc.just_for_test

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext._


object HelloWorld {
        def main(args:Array[String]) {
                if (args.length != 2) {
                        println("ERROR: you need two arguments:")
                        println("USAGE: HelloWorld inputPath outputPath")
                        System.exit(-1)
                }
                val Array(inputPath, outputPath) = args
                val conf = new SparkConf().setAppName("hello")
                val sc = new SparkContext(conf)
                val file = sc.textFile(inputPath)
                val words = file.flatMap(_.split(":")).flatMap(_.split(","))
                val a = words.map(a=>(a,1))
                val a1 = a.reduceByKey(_+_)
                a1.saveAsTextFile(outputPath)
                println(a1)

                sc.stop()
        }

}
