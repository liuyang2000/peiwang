package test

import java.text.SimpleDateFormat
import java.util.Date
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.hive.HiveContext
import com.bmsoft.util.CommProperties

/**
 * 按单位（县/班组）馈线日统计表
 *
 * @author mathsyang
 */
object TestError {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("TestError")
      .set("spark.shuffle.reduceLocality.enabled", "false")
      .set("spark.eventLog.enabled", "false"))
    val hc = new HiveContext(sc)

    val sdf = new SimpleDateFormat(CommProperties.FORMAT_DATE_DAY)
    val date = sdf.parse("20170101")
    
    for (i <- 0 to 360) {
      date.setTime(date.getTime + 24 * 3600 * 1000)
      val dt = sdf.format(date)

      try {
        val data = hc.sql(s"select * from pwyw.pwyw_pbydyc where dt = ${dt}")
        data.show(false)
      } catch {
        case e: Exception => {
          println(s"alter table pwyw.pwyw_pbydyc drop partition(dt='${dt}');")
        }
      }
    }
  }
}