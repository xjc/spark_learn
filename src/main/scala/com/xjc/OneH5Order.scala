package com.xjc
import org.apache.spark.{SparkConf,SparkContext}

object OneH5Order {
    def main(args:Array[String]) {
        //val conf = new SparkConf().setAppName("test").setMaster("local[10]")
        val conf = new SparkConf().setAppName("test").setMaster("local[4]")
	val sc = new SparkContext(conf)
	val data_dir = List("hdfs://Namenode01.Hadoop:9000/user/hive/warehouse/dwd.db/i_t_order/statmonth=2015-10",
	"hdfs://Namenode01.Hadoop:9000/user/hive/warehouse/dwd.db/i_t_order/statmonth=2015-11" ,
	"hdfs://Namenode01.Hadoop:9000/user/hive/warehouse/dwd.db/i_t_order/statmonth=2015-12" ,
	"hdfs://Namenode01.Hadoop:9000/user/hive/warehouse/dwd.db/i_t_order/statmonth=2016-01" ,
	"hdfs://Namenode01.Hadoop:9000/user/hive/warehouse/dwd.db/i_t_order/statmonth=2016-02" ,
	"hdfs://Namenode01.Hadoop:9000/user/hive/warehouse/dwd.db/i_t_order/statmonth=2016-03").mkString(",")

        val data = sc.textFile(data_dir).map(_.split("\001")).map(a=>(a(7),(a(35), a(36), a(46)))).filter(_._2._1 == "1")
	data.cache
	val oneOrder = data.map(a=>(a._1,1)).reduceByKey(_ + _).filter(_._2 == 1)
	val h5Order = data.filter(_._2._2 == "02").map(a=>(a._1, a._2._3.substring(0,7)))
	val result = oneOrder.join(h5Order).map(a=>a._1 + "," + a._2._2)
	result.count

	result.saveAsTextFile("/data3/xjc/data/h5_order_20160415")

	sc.stop()
    }
}
