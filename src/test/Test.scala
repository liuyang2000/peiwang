package test

import java.util.Date
import java.text.SimpleDateFormat
import com.bmsoft.util.CommProperties

/**
 * 按单位（县/班组）馈线曲线采集完整率日统计表
 * @author mathsyang
 */
object KXBZGatherSuccDay {
  def StringToTimeStamp(s: Any): java.sql.Timestamp = {
    val sdf = new SimpleDateFormat(CommProperties.FORMAT_TIMESTAMP)
    try {
      val date = s.asInstanceOf[String]
      val dd = sdf.parse(date).getTime
      println(dd)
      new java.sql.Timestamp(sdf.parse(date).getTime)
    } catch {
      case e: Exception => {
        println(e.getMessage)
        null
      }
    }
  }
    
  def main(args: Array[String]): Unit = {
        
    val date = new Date
    val sdf = new SimpleDateFormat(CommProperties.FORMAT_DATE_DAY)
    val date2 = sdf.parse("20170501")
    
    println((date.getTime - date2.getTime) / (24 * 3600 * 1000))

  }
}