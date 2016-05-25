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

object userLifeModule {

  def main(args: Array[String]) {
    //val conf = new SparkConf().setAppName("test").setMaster("local[10]")
    val conf = new SparkConf().setAppName("viscosityCust")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val hiveContext = new HiveContext(sc)
    import sqlContext.implicits._
    import hiveContext.implicits._
    import hiveContext.sql

    val days1 = TimeUtils.getNdaysAgo(1)

    val blackDf = sql("select distinct(customer_phone) phone from stg.t_strategy_order where identify_status=2")
    blackDf.coalesce(1).cache
    sc.broadcast(blackDf)
    blackDf.registerTempTable("black")

    val vipDf = sql("""
select distinct phone
from
(
select phone from tmp_rjg.t_user_basic_level where user_level=10
union all
select phone from stg.t_vip
) t1
	""")
    vipDf.coalesce(1).cache
    sc.broadcast(vipDf)
    vipDf.registerTempTable("vip")

    val driverDf = sql("""
select distinct phone from (
        select phone from stg.t_driver union all
        select ext_phone phone from stg.t_driver union all
        select contact_phone from stg.t_driver_recruitment) i
        """)
    driverDf.coalesce(1).cache
    sc.broadcast(driverDf)
    driverDf.registerTempTable("driver")

    val baiduDf = sql("""select user_id from app.app_user_baidu_user_info""")
    baiduDf.coalesce(1).cache
    sc.broadcast(baiduDf)
    baiduDf.registerTempTable("baidu")

    val cityDf = sql("select city_id, city_name from stg.t_city_config")
    cityDf.cache
    sc.broadcast(cityDf)
    cityDf.registerTempTable("t_city")

    val updateSql = s"""
insert overwrite table app.app_user_life_cycle_sum_da_for_sale_43city_bak partition(dt='$days1')
select
  a.user_flag,
  c.city_name,
  a.user_id,
  a.phone
from (
select
  user_id,
  user_flag,
  phone
from (
  select user_id,user_flag, phone
  from (
     select
        a.user_id,
       a.user_flag,
       b.phone
     from (select * from app.app_user_life_cycle_all_user_info_type4_22 ) a
     inner join (
       select
        user_id,
        phone
       from db_sensitive.sens_user_info
      where dt = '$days1'
    ) b on a.user_id = b.user_id
    left outer join
    (
      select phone from black
    ) d
    on b.phone=d.phone
    left outer join
    (select phone from vip) v
      on b.phone=v.phone
    left outer join driver h
      on b.phone=h.phone
    left outer join baidu i
      on a.user_id = i.user_id
    where length(b.phone)=11 and substr(b.phone,1,1)=1 and substr(b.phone,2,1) in(3,5,7,8)
      and d.phone is null and h.phone is null and v.phone is null and i.user_id is null
  ) t
) t
) a
left outer join (select user_id, real_city_id from dwd.dwd_m03_user_profile_da where statday = '$days1') b
  on a.user_id = b.user_id
join t_city c
  on b.real_city_id = c.city_id 

        """

    sql(updateSql)

    sc.stop()
  }
}
