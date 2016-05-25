package com.xjc.utils
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

object udfmd5 {

  implicit def long2str(l: Long): String = String.valueOf(l)
  implicit def int2str(l: Int): String = String.valueOf(l)
  implicit def byte2str(l: Byte): String = String.valueOf(l)
  implicit def double2str(l: Double): String = String.valueOf(l)

  val hexDigits = Array[String]("0", "1", "2", "3", "4", "5", "6", "7", "8", "9",
    "a", "b", "c", "d", "e", "f");

  def byteArrayToHexString(b: Array[Byte]): String = {
    b.map(byteToHexString).mkString
  }

  def byteToHexString(b: Byte): String = {
    var n = if (b < 0) 256 + b else b
    var (d1, d2) = (n / 16, n % 16)
    hexDigits(d1) + hexDigits(d2)
  }

  def getMd5(toEncrypt: String, seed: String = ""): Option[String] = {
    try {
      var results = MessageDigest.getInstance("MD5").digest((toEncrypt + seed).getBytes());
      var resultString = byteArrayToHexString(results)
      Some(resultString)
    } catch {
      case e: NoSuchAlgorithmException =>
        println(e.toString)
        None
    }
  }

  /**
   * def main(args:Array[String]) {
   * val phone = "15010380354"
   * println(getMd5(phone))
   * println(getMd5(phone, "aa"))
   * }
   */

}
