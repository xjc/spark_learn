package com.edj.util

/**
 * Created by xjc on 16-4-1.
 */
object Decode {

  import java.net.URLDecoder
  import java.nio.charset.Charset

  trait Extract {
    def charset: Charset

    def unapply(raw: String) =
    //try(URLDecoder.decode(raw, charset.name())).toOption
      try (Option(URLDecoder.decode(raw, charset.name())))
  }

  object utf8 extends Extract {
    val charset = Charset.forName("utf8")
  }

  def main(args: Array[String]): Unit = {
    val a = "2016-03-30+12%3A04%3A35"
    val c = a match {
      case Decode.utf8(b) => b
      case _ => a
    }
    println(c)
  }
}
