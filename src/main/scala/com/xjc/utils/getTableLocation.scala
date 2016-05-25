package com.xjc.utils
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.Row

object getTableLocation {
  def getLocation(tableName: String, hc: HiveContext): String = {
    try {
      val result = hc.sql(s"desc formatted $tableName")
      val location = result.collect.map { case Row(a) => a.toString }.
        filter { _.startsWith("Location") }.map(_.split("Location:")(1).trim)
      location(0)
    } catch {
      case e: Exception => {
        println("ERROR: get $tableName location" + e.toString)
        sys.exit(-1)
      }
    }
  }

  def getColumns(tableName: String, hc: HiveContext): Array[String] = {
    Array("a")

  }

  def main(args: Array[String]) {
    /**
     * if (args.length != 1) {
     * println("usage: getTableLocation <tableName>")
     * println("eg:    getTableLocation tmp.t20160501")
     * println("or:    ")
     * System.exit(-1)
     * }
     */

    //val tableName = args(0).trim
    val tableName = "dwd.i_t_order"
    val conf = new SparkConf().setAppName("test").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val hc = new HiveContext(sc)
    val location = getLocation(tableName, hc)
    println(location)

    sc.stop
  }
}
