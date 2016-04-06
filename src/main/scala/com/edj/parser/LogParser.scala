package com.edj.parser

import java.text.SimpleDateFormat
import java.util.Locale
import com.edj.util.Decode


/**
 * Created by xjc on 16-3-30.
 */
object LogParser {

  val post_regrex = """(\w+)\s(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})\s-\s-\s\[(.+)\]\s(POST|GET)\s(.+)\sHTTP\/\d\.\d\s(\d{3})\s(\d+)\s(.+?)\s(.+?)\s*\d{1,2}\.\d{3,}\s*(.+?)\s*?((?:(?:2[0-4]\d|25[0-5]|[01]?\d\d?)\.){3}(?:2[0-4]\d|25[0-5]|[01]?\d\d?))$""" r
  val keySet = Set("method", "appkey", "token", "app_ver", "timestamp", "from", "os", "login_phone", "td_appcpa_channel", "current_city_id", "macaddress", "from_type", "udid", "ver", "model", "sig", "source_type")

  def unapply(str: String): Option[String] = {
    parseLog(str)
  }

  def parseLog(log: String): Option[String] = {
    var formatLogStr = ""
    var result: Option[String] = None

    log match {
      case post_regrex(jobid, ip, tm, method, http_url, http_status_code, bytes_sent, http_referer, post_params, agent, client_ip) => {
        //        println(jobid, ip, formatTime(tm), method, http_url, http_status_code, bytes_sent, http_referer, post_params, agent, client_ip)
        //        println(http_url)
        var (url, params) = ("", "")
        params = if (method == "GET") {
          if (http_url.indexOf('?') != -1) http_url.split('?')(1) else ""
        } else post_params

        val paramsMap = getParamsMap(params)
        paramsMap.put("http_referer", http_referer)
        paramsMap.put("agent", http_referer)
        paramsMap.put("client_ip", http_referer)
        paramsMap.put("bytes_sent", bytes_sent)

        url = if (method == "GET") paramsMap.getOrElse("method", "")
        else {
          if (http_url.contains("method=")) http_url.split("method=")(1) else ""
        }

        val otherMapStr = getOtherParamsMap(paramsMap)

        formatLogStr = (jobid :: ip :: formatTime(tm) :: method :: url ::
          paramsMap.getOrElse("appkey", "") ::
          paramsMap.getOrElse("token", "") ::
          paramsMap.getOrElse("app_ver", "") ::
          paramsMap.getOrElse("timestamp", "") ::
          paramsMap.getOrElse("from", "") ::
          paramsMap.getOrElse("os", "") ::
          paramsMap.getOrElse("login_phone", "") ::
          paramsMap.getOrElse("td_appcpa_channel", "") ::
          paramsMap.getOrElse("current_city_id", "") ::
          paramsMap.getOrElse("macaddress", "") ::
          paramsMap.getOrElse("from_type", "") ::
          paramsMap.getOrElse("udid", "") ::
          paramsMap.getOrElse("ver", "") ::
          paramsMap.getOrElse("model", "") ::
          paramsMap.getOrElse("sig", "") ::
          paramsMap.getOrElse("source_type", "") ::
          otherMapStr ::
          Nil).mkString("\001")
        //          Nil).mkString("\n")

        result = Some(formatLogStr)
      }
      case _ => None
    }
    result
  }

  def getParamsMap(str: String): scala.collection.mutable.Map[String, String] = {
    val map = new scala.collection.mutable.HashMap[String, String]()
    val urlUnquote = str match {
      case Decode.utf8(a) => a
      case _ => str
    }

    for (i <- urlUnquote.split('&')) {
//      println("!" * 100)
//      println(i)
//      println("!" * 100)
      if ( i.contains('=')) {
        val key_val = i.split('=')
        map.put(key_val(0), if(key_val.length == 2) key_val(1) else "")
      }
    }
    return map
  }

  def getOtherParamsMap(paramsMap: scala.collection.mutable.Map[String, String]): String = {
    paramsMap.filter(i => (!(keySet.contains(i._1)))).map(i => i._1 + "\003" + i._2).mkString("\002")
  }

  def main(args: Array[String]): Unit = {
    val log = "api12 10.158.20.8 - - [30/Mar/2016:12:04:34 +0800] GET /rest?app_ver=4.5.1.0&appkey=10000002&from=internal&gps_type=baidu&latitude=39.918544&longitude=116.61589&mac=e0%3A19%3A1d%3A6a%3A43%3A95&macaddress=12%3A34%3A56%3A78%3A9A%3ABC&method=d.nearby&model=HUAWEI-Che1-CL10&os=HUAWEI19%2C4.4.4&timestamp=2016-03-30%2012%3A04%3A33&token=fdb4f3e1d0645f9a7ac57d5b71afe418&udid=A000004FA9D2BB&ver=3&sig=1e9cb404d5f8e46381f5802fba6f581d HTTP/1.0 200 1151 - -  0.178 okhttp/2.5.0 117.73.17.249"
    print(LogParser.parseLog(log))
  }


  //[30/Mar/2016:12:04:34
  def formatTime(timeStr: String): String = {
    val loc = new Locale("en")
    val old_fm = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss +0800", loc)
    val new_fm = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    val dt2 = new_fm.format(old_fm.parse(timeStr))
    dt2
  }

}
