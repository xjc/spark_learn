package com.xjc
import org.apache.spark.{ SparkConf, SparkContext }
import com.xjc.utils.TimeUtils

object Orders {
  def main(args: Array[String]) {
    //val conf = new SparkConf().setAppName("test").setMaster("local[10]")
    val conf = new SparkConf().setAppName("test").setMaster("local[4]")
    val sc = new SparkContext(conf)
    val months0 = TimeUtils.getNmonthsAgo(0)
    val hdfs = s"hdfs://Namenode01.Hadoop:9000/user/hive/warehouse/dwd.db/i_t_order/statmonth=$months0"
    val data = sc.textFile(hdfs)

    val orders = data.map((a: String) => { a.split("\001")(46) -> a.split("\001")(35) })
    orders.cache

    /**
     * orders.map(_._1 -> 1).reduceByKey(_ + _).sortBy(_._1,true,1).foreach(println)
     * println("#"*50)
     * orders.filter(a=>a._2 == "1").map(a=>a._1->1).reduceByKey(_+_).sortBy(_._1, true, 1).foreach(println)
     */

    val all_orders = orders.map(_._1 -> 1).reduceByKey(_ + _)
    val finished_orders = orders.filter(a => a._2 == "1").map(a => a._1 -> 1).reduceByKey(_ + _)
    all_orders.join(finished_orders).sortBy(_._1, true, 1).foreach {
      a =>
        {
          println("statday:\t" + a._1 + "\t" + a._2._1 + "\t" + a._2._2)
        }
    }

    sc.stop()
  }
}
