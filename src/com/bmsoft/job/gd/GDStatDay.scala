package com.bmsoft.job

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.hive.HiveContext

import com.bmsoft.util.DateFormatUtil
import com.bmsoft.impl.GDStatImpl
import com.bmsoft.jdbc.GDStatJdbc
import com.bmsoft.util.CommProperties

/**
 * 按单位（县/班组）馈线日统计表
 *
 * @author mathsyang
 */
object GDStatDay {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("GDStatDay")
        .set("spark.shuffle.reduceLocality.enabled", "false")
        .set("spark.eventLog.enabled","false"))
    val hc = new HiveContext(sc)

    val date = new Date
    date.setTime(date.getTime - 24 * 3600 * 1000)
    val sdf = new SimpleDateFormat(CommProperties.FORMAT_DATE_DAY)
    val dt = if (args.length == 0 || args(0) == null) sdf.format(date) else args(0)

    val stat = new GDStatImpl(hc, dt, DateFormatUtil.STAT_CYCLE_DAY)
    val df1 = stat.statGDDetail.repartition(8)

    val jdbc = new GDStatJdbc()
    jdbc.insertGDStatList(df1, dt, DateFormatUtil.STAT_CYCLE_DAY)
  }
}