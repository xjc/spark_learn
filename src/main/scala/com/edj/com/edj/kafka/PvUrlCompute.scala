package com.edj.com.edj.kafka

import java.sql.{Timestamp, DriverManager, PreparedStatement, Connection}
import java.util.Date

import org.apache.spark.{rdd, SparkConf}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * Created by xjc on 16-4-5.
 */
object PvUrlCompute extends App {
  val zkQuorum = "localhost:2181"
  val group = "group1"
  val sparkConf = new SparkConf().setAppName("ApiLogPvCount").setMaster("local[4]")
  val ssc = new StreamingContext(sparkConf, Seconds(10))

  val topicsMap = Map("nginx_api_log_wash2" -> 2)
  val lines = KafkaUtils.createStream(ssc, zkQuorum, group, topicsMap).map(_._2)

  val windows_effect_lines = lines.window(Seconds(30), Seconds(10))

  val (mysql_url, user_name, password) = ("jdbc:mysql://localhost:3306/log_analysis", "xjc", "jie0512")

  lines.foreachRDD(
    rdd => {
      val all_pv = rdd.count()


      var conn: Connection = null
      var ps: PreparedStatement = null
      val all_pv_sql = "insert into pv_analysis(time1, log_total, effective_pv) values (?, ?, ?)"
      val url_pv_sql = "insert into pv_url_analysis(time1, log_total, method) values (?, ?, ?)"
      try {

        conn = DriverManager.getConnection(mysql_url, user_name, password)
        ps = conn.prepareStatement(all_pv_sql)
        val now = new Timestamp(new Date().getTime())
        ps.setTimestamp(1, now)
        ps.setLong(2, all_pv)
        ps.setLong(3, 0)
        ps.executeUpdate()

        ps = conn.prepareStatement(url_pv_sql)

        /**
        rdd.map{
          str => {
            (str.split("\001")(4).trim, 1)
          }
        }.reduceByKey(_ + _).foreach { i =>
          println("#" * 100)
          println(i)
          println(i._1)
          println(i._2)
          println("#" * 100)
          ps.setTimestamp(1, now)
          ps.setLong(2, i._2)
          ps.setString(3, i._1)
          ps.executeUpdate()
        }
          */

        rdd.map {
          str => {
            str.split("\001")(4).trim -> 1
          }
        }.reduceByKey(_ + _).collect().filter(i => i._1 != "").
          foreach(i => {
            ps = conn.prepareStatement(url_pv_sql)
            ps.setTimestamp(1, now)
            ps.setLong(2, i._2)
            ps.setString(3, i._1)
            ps.executeUpdate()
          })

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

      println("#" * 50)
      println(all_pv)
      println("#" * 50)


    }
  )

  windows_effect_lines.foreachRDD(
    rdd => {
      rdd.foreachPartition(
        record => {
          println("@" * 50)
          println(record.size)
          println("@" * 50)
          //          Class.forName("com.mysql.jdbc.Driver").newInstance()
        }

      )
    }
  )


  ssc.start
  ssc.awaitTermination

}
