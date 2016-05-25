package com.xjc.transformation

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import java.sql.{Timestamp, DriverManager, PreparedStatement, Connection}

object MapPartitionsTest {
      def main(args:Array[String]) {
              val conf = new SparkConf().setAppName("MapPartitionsTest").setMaster("local[8]")
              val sc = new SparkContext(conf)
              val data = 1 to 10000
              val rdd = sc.parallelize(data, 8)
              //map test
              val (mysql_url, user_name, password) = ("jdbc:mysql://localhost:3306/test", "xjc", "jie0512")

              rdd.mapPartitions(
                  record=>{
                  var conn:Connection = null
                  var ps: PreparedStatement = null

                  val sql_str = "insert into t_map(id, time_dt) values (?, now())"

                  try {
                        conn = DriverManager.getConnection(mysql_url, user_name, password)
                        ps = conn.prepareStatement(sql_str)
                        while (record.hasNext) {
                            ps.setInt(1, record.next)
                            ps.executeUpdate()
                        }
                  } catch {
                        case e: Exception => println(e.toString)
                  } finally {
                      if (ps != null) {
                          ps.close()
                      }
                      if (conn != null) {
                          conn.close()
                      }
                  }
                  record
                  }
              ).count   //action
              sc.stop
  }
}
