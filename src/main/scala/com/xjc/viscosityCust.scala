package com.xjc
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import java.net.URI
import com.xjc.utils.udfmd5
import com.xjc.utils.TimeUtils

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.Row

object viscosityCust {

  case class singleRecord(phone: String, city_id: String, first_ord_date: String, last_ord_date: String, if_action: Int)

  def main(args: Array[String]) {
    //val conf = new SparkConf().setAppName("test").setMaster("local[10]")
    val conf = new SparkConf().setAppName("viscosityCust")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val hiveContext = new HiveContext(sc)

    val days30 = TimeUtils.getNdaysAgo(30)
    val days60 = TimeUtils.getNdaysAgo(60)
    val days90 = TimeUtils.getNdaysAgo(90)
    val days180 = TimeUtils.getNdaysAgo(180)
    val days360 = TimeUtils.getNdaysAgo(360)
    val days1 = TimeUtils.getNdaysAgo(1)

    val regeditFile = "hdfs://Namenode01.Hadoop:9000/user/hive/warehouse/stg.db/t_customer_main/";
    val mainData = sc.textFile(regeditFile)
    /**
     * val mainRdd = mainData.map(_.split("\001")).map{
     * case Array(id,name,gender,birthday,phone,email,car_num,imei,backup_phone,city_id,credit,activity,amount,coupon,
     * type1,company,vip_card,vip_main,vip_card_balance,invoice_title,invoice_remark,bill_receive_mode,status,
     * is_verify,channel,remark,address,operator,create_time,update_time,
     * last_login, _*) => (phone, city_id, update_time, last_login)
     * }
     * //unapply pattern, maximum = 22
     */
    val mainRdd = mainData.map(_.split("\001")).map { i => (i(4).trim, (i(9), i(29), i(30)))
    }.coalesce(10)

    val loginRdd = mainRdd.filter(i => i._2._2 != "null" && i._2._2 >= days30 && i._2._3 != "null" && i._2._3 >= days30).
      map(_._1).distinct

    val m03File = s"hdfs://Namenode01.Hadoop:9000/user/hive/warehouse/dwd.db/dwd_m03_user_profile_da/statday=$days1/"
    val m03Data = sc.textFile(m03File)
    val m03Rdd = m03Data.map(_.split("\001")).map {
      case Array(user_id, update_time, create_time, gender, email, birth_dt, ext_phone, imei, is_driver,
        phone_ismobile, reg_city_id, reg_city_level, real_city_id, _*) => (user_id.trim, real_city_id)
    }.coalesce(10)

    val m00File = "hdfs://Namenode01.Hadoop:9000/user/hive/warehouse/dwd.db/dwd_m00_user_order_info/"
    val useridPhoneData = sc.textFile(m00File)
    val useridPhoneRdd = useridPhoneData.map(_.split("\001")).map {
      case Array(phone, first_ord_id, first_ord_time, first_ord_date, first_sign_amount, first_ord_city_id,
        first_ord_channel, first_ord_original_type, first_ord_timerange, latest_ord_id,
        latest_ord_time, _*) => (phone, first_ord_date, latest_ord_time.substring(0, 10))
    }.
      map(i => (udfmd5.getMd5(i._1.trim, "EDJ-DW-1024").getOrElse(""), (i._1.trim, i._2, i._3)))

    //(userid, (phone,first_ord_date, last_ord_date), (real_city_id)) => (phone, (first_ord_date, last_ord_date, real_city_id))
    val orderPhoneRdd2 = useridPhoneRdd.join(m03Rdd).map(i => (i._2._1._1, (i._2._1._2, i._2._1._3, i._2._2)))
    //city_id  first_ord_date  last_ord_date
    //(Option[(String, String, String)], Option[(String, String, String)])
    val allPhoneRdd = orderPhoneRdd2.fullOuterJoin(mainRdd).map(
      i => {
        (i._1, if (i._2._1 != None) (i._2._1.get._3, i._2._1.get._1, i._2._1.get._2)
        else (i._2._2.get._1, "", ""))
      })

    //dwa.app_all_phone_city
    val allPhoneRdd_bak = allPhoneRdd.map(i => (i._1 :: i._2._1 :: Nil).mkString(",")).coalesce(10)
    val hfds_location_phone = "hdfs://Namenode01.Hadoop:9000/user/hive/warehouse/dwa.db/app_all_phone_city"
    val fs = FileSystem.get(new URI("hdfs://Namenode01.Hadoop:9000"), new Configuration())
    fs.delete(new Path(hfds_location_phone))
    allPhoneRdd_bak.saveAsTextFile(hfds_location_phone)

    //login action

    val status = fs.listStatus(new Path("/user/hive/warehouse/db_log_parase.db/t_nearby_log/"))
    val fileList = status.map(_.getPath.toString).filter(_.contains("dt=")).
      filter(_.split("dt=")(1) >= days30).mkString(",")
    val nearbyData = sc.textFile(fileList)
    val re = """.*udid=(\w+),?.*"""r
    val nearbyRdd = nearbyData.map(_.split("\t")(1)).map {
      case re(x) => x
      case _ => ""
    }.filter(_ != "").distinct.map(i => (i, "")).coalesce(10)

    //token rdd
    val tokenFile = "hdfs://Namenode01.Hadoop:9000/user/hive/warehouse/stg.db/t_customer_token/"
    val tokenData = sc.textFile(tokenFile)
    val tokenRdd = tokenData.map(_.split("\01")).map(i => (i(3), i(1))) //udid phone 
    val tokenPhoneRdd = tokenRdd.join(nearbyRdd).map(_._2._1).distinct
    val loginAllRdd = tokenPhoneRdd.union(loginRdd).distinct.coalesce(10)

    val status1 = fs.listStatus(new Path("/user/hive/warehouse/dwd.db/i_t_order/"))
    val order30FileList = status1.map(_.getPath.toString).filter(_.contains("statmonth=")).
      filter(_.split("statmonth=")(1) >= days30.substring(0, 7)).mkString(",")
    val order30Data = sc.textFile(order30FileList)
    val order30Rdd = order30Data.map(_.split("\001")).map {
      i => if (i(46) >= days30) i(6) else ""
    }.filter(_ != "").distinct

    val actionRdd = order30Rdd.union(loginAllRdd).distinct.map(i => (i, 1)).coalesce(10)
    //val allDataRdd = allPhoneRdd.leftOuterJoin(actionRdd).map{
    //    i=>(i._1 :: i._2._1._1 :: i._2._1._2 :: i._2._1._3 :: i._2._2.getOrElse(0) :: Nil).mkString(",")
    /**
     * val fileLoc = s"/tmp/$days1/just_for_test"
     * allDataRdd.saveAsTextFile(fileLoc)
     */
    //}

    //phone,city_id,first_ord_date,last_ord_date,if_action
    import sqlContext.implicits._

    //implicit def int2str(num:Int):String = String.valueOf(num)
    val allDataRdd = allPhoneRdd.leftOuterJoin(actionRdd).map {
      i => (i._1, i._2._1._1, i._2._1._2, i._2._1._3, i._2._2.getOrElse(0))
    }
    //allDataRdd.saveAsTextFile("/tmp/all_data_20160513_1")
    val allDataDf = allDataRdd.map(i => singleRecord(i._1, i._2, i._3, i._4, i._5)).toDF
    allDataDf.registerTempTable("alldata")
    val sql1 = s"""
select distinct phone, city_id, flag
  from (
select phone,city_id, 
       case
         when first_ord_date >= "$days30" then "A"
         when first_ord_date < "$days30" and last_ord_date >= "$days30" then "B"
         when last_ord_date >= "$days60" and last_ord_date < "$days30" and if_action != 0 then "C"
         when last_ord_date >= "$days60" and last_ord_date < "$days30" and if_action = 0 then "D"
         when last_ord_date >= "$days90" and last_ord_date < "$days60" and if_action != 0 then "E"
         when last_ord_date >= "$days90" and last_ord_date < "$days60" and if_action = 0 then "F"
         when last_ord_date >= "$days180" and last_ord_date < "$days90" and if_action != 0 then "G"
         when last_ord_date >= "$days180" and last_ord_date < "$days90" and if_action = 0 then "H"
         when last_ord_date >= "$days360" and last_ord_date < "$days180" and if_action != 0 then "I"
         when last_ord_date >= "$days360" and last_ord_date < "$days180" and if_action = 0 then "J"
         when last_ord_date < "$days360" and if_action != 0 then "K"
         when last_ord_date < "$days360" and if_action = 0 then "L"
         else ''
       end as flag
  from alldata 
) a
 where a.flag <> ''
"""
    print(sql1)
    val allDataResult = sqlContext.sql(sql1).rdd.map(_.mkString(","))
    //val hiveDataLocation = "/tmp/all_data_20160513"
    val hiveDataLocation = "hdfs://Namenode01.Hadoop:9000/user/hive/warehouse/tmp_dev.db/t_viscosity_cust_tmp/"
    fs.delete(new Path(hiveDataLocation))
    allDataResult.saveAsTextFile(hiveDataLocation)

    val loadHiveCmd = s"""load data inpath '$hiveDataLocation/*' overwrite into table tmp_dev.t_viscosity_cust"""
    hiveContext.sql(loadHiveCmd)

    //(phone, (city_id  first_ord_date  last_ord_date))
    /**
     * val allDataRdd = allPhoneRdd.leftOuterJoin(actionRdd).map{
     * i=>{ if (i._2._2 >= "2016-04-09" ) (i._1 :: i._2._1 :: i._2._2 :: i._2._3 :: "A" :: Nil).mkString(",")
     * else if (i._2._2 < "2016-04-09" && i._2._3 >= "2016-04-09" ) (i._1 :: i._2._1 :: i._2._2 :: i._2._3 :: "B" :: Nil).mkString(",")
     * else if (i._2._3 >= "-60" && i._2._3 <= "-31" && i._2._4 != None) (i._1 :: i._2._1 :: i._2._2 :: i._2._3 :: "C" :: Nil).mkString(",")
     * else if (i._2._3 >= "-60" && i._2._3 <= "-31" && i._2._4 == None) (i._1 :: i._2._1 :: i._2._2 :: i._2._3 :: "D" :: Nil).mkString(",")
     * else if (i._2._3 >= "-90" && i._2._3 <= "-61" && i._2._4 == None) (i._1 :: i._2._1 :: i._2._2 :: i._2._3 :: "E" :: Nil).mkString(",")
     * else if (i._2._3 >= "-90" && i._2._3 <= "-61" && i._2._4 == None) (i._1 :: i._2._1 :: i._2._2 :: i._2._3 :: "F" :: Nil).mkString(",")
     *
     * else if (i._2._3 >= "-90" && i._2._3 <= "-61" && i._2._4 == None) (i._1 :: i._2._1 :: i._2._2 :: i._2._3 :: "G" :: Nil).mkString(",")
     * else if (i._2._3 >= "-90" && i._2._3 <= "-61" && i._2._4 == None) (i._1 :: i._2._1 :: i._2._2 :: i._2._3 :: "H" :: Nil).mkString(",")
     * else if (i._2._3 >= "-90" && i._2._3 <= "-61" && i._2._4 == None) (i._1 :: i._2._1 :: i._2._2 :: i._2._3 :: "I" :: Nil).mkString(",")
     * else if (i._2._3 >= "-90" && i._2._3 <= "-61" && i._2._4 == None) (i._1 :: i._2._1 :: i._2._2 :: i._2._3 :: "J" :: Nil).mkString(",")
     *
     * else if (i._2._3 >= "-90" && i._2._3 <= "-61" && i._2._4 == None) (i._1 :: i._2._1 :: i._2._2 :: i._2._3 :: "K" :: Nil).mkString(",")
     * else if (i._2._3 >= "-90" && i._2._3 <= "-61" && i._2._4 == None) (i._1 :: i._2._1 :: i._2._2 :: i._2._3 :: "M" :: Nil).mkString(",")
     * else ""
     * }
     * }.filter(_ != "")
     */

    sc.stop()
  }
}
