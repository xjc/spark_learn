package com.xjc.transformation

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import com.xjc.utils.getPartitionsDataTest._

/**
def pipe(command: Seq[String], env: Map[String, String] = Map(), printPipeContext: ((String) ⇒ Unit) ⇒ Unit = null, printRDDElement: (T, (String) ⇒ Unit) ⇒ Unit = null, separateWorkingDir: Boolean = false): RDD[String]
Return an RDD created by piping elements to a forked external process. The print behavior can be customized by providing two functions.
 Pipe each partition of the RDD through a shell command, e.g. a Perl or bash script. RDD elements are written to the process's stdin and lines output to its stdout are returned as an RDD of strings.
 */

object pipeTest {
    def main(args:Array[String]) {
        val conf = new SparkConf().setAppName("pipeTest").
            setMaster("local[2]")
            val sc = new SparkContext(conf)
            val data = 1 to 10
            val rdd = sc.parallelize(data, 2)    

            var rdd1 = rdd.pipe("wc -l")
            printParSet(rdd)
            rdd1.collect.foreach(println)


            sc.stop
    }
}
