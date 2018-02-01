package com.bmsoft.job

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.hive.HiveContext

import com.bmsoft.impl.GDStatImpl
import com.bmsoft.jdbc.GDStatJdbc
import com.bmsoft.util.DateFormatUtil

/**
 * 馈线统计
 *
 * @author mathsyang
 */
object GDStatHYear {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("GDStatHYear")
        .set("spark.shuffle.reduceLocality.enabled", "false")
        .set("spark.eventLog.enabled","false"))
    val hc = new HiveContext(sc)

    val dt = if (args.length == 0 || args(0) == null) DateFormatUtil.dateHY(new Date, 1)
    else if (args(0).length() == 6) args(0)
    else DateFormatUtil.dateHY(args(0), 0)

    val stat = new GDStatImpl(hc, dt, DateFormatUtil.STAT_CYCLE_HYEAR)
    val df = stat.statGDDetail.repartition(8)

    val jdbc = new GDStatJdbc()
    jdbc.insertGDStatList(df, dt, DateFormatUtil.STAT_CYCLE_HYEAR)
  }
}