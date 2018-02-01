package com.bmsoft.job

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.hive.HiveContext

import com.bmsoft.impl.PBStatImpl
import com.bmsoft.jdbc.PBStatJdbc
import com.bmsoft.util.DateFormatUtil
import com.bmsoft.util.CommProperties

/**
 * 按单位（县/班组）馈线日统计表
 *
 * @author mathsyang
 */
object PBStatDay {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("PBStatDay")
      .set("spark.shuffle.reduceLocality.enabled", "false")
      .set("spark.eventLog.enabled", "false"))
    val hc = new HiveContext(sc)

    val date = new Date
    date.setTime(date.getTime - 24 * 3600 * 1000 * 2)
    val sdf = new SimpleDateFormat(CommProperties.FORMAT_DATE_DAY)
    val dt = if (args.length == 0 || args(0) == null) sdf.format(date) else args(0)

    val tabs = if (args.length >= 2 && args(1) != null) args(1) else "1,2,3,4,5,6,7"

    val stat = new PBStatImpl(hc, dt, DateFormatUtil.STAT_CYCLE_DAY)
    val jdbc = new PBStatJdbc()
    if (tabs.contains("1")) {
      val df1 = stat.statRunStatus.repartition(8).persist()
      jdbc.insertRunStat(df1, dt, DateFormatUtil.STAT_CYCLE_DAY)
      df1.unpersist()
    }
    if (tabs.contains("2")) {
      val df2 = stat.statExcept.repartition(8).persist()
      jdbc.insertPBExcept(df2, dt, DateFormatUtil.STAT_CYCLE_DAY)
      df2.unpersist()
    }
    if (tabs.contains("4")) {
      val df4 = stat.statSSGatherSuccRate.repartition(8).persist()
      jdbc.insertPBSSGatherSucc(df4, dt, DateFormatUtil.STAT_CYCLE_DAY)
      df4.unpersist()
    }
    if (tabs.contains("6")) {
      val df6 = stat.statBZGatherSuccRate.repartition(8).persist()
      jdbc.insertPBBZGatherSucc(df6, dt, DateFormatUtil.STAT_CYCLE_DAY)
      df6.unpersist()
    }
    if (tabs.contains("3")) {
      val df3 = stat.statSSExcept.repartition(8).persist()
      jdbc.insertSSPBExcept(df3, dt, DateFormatUtil.STAT_CYCLE_DAY)
      df3.unpersist()
    }
    if (tabs.contains("5")) {
      val df5 = stat.statBZExcept.repartition(8).persist()
      jdbc.insertBZPBExcept(df5, dt, DateFormatUtil.STAT_CYCLE_DAY)
      df5.unpersist()
    }
    if (tabs.contains("7")) {
      val df7 = stat.statSSClass.repartition(8).persist()
      jdbc.insertPBSSClass(df7, dt, DateFormatUtil.STAT_CYCLE_DAY)
      df7.unpersist()
    }
  }
}