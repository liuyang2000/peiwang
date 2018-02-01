package com.bmsoft.job

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.hive.HiveContext

import com.bmsoft.util.DateFormatUtil
import com.bmsoft.jdbc.GDKKXStatJdbc
import com.bmsoft.impl.GDKKXStatImpl
import com.bmsoft.util.CommProperties

/**
 * gdkkx day-stat submit
 *
 * @author mathsyang
 */
object GDKKXStatDay {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("GDKKXStatDay")
        .set("spark.shuffle.reduceLocality.enabled", "false")
        /*.set("spark.eventLog.enabled","false")*/)
    val hc = new HiveContext(sc)

    val date = new Date
    date.setTime(date.getTime - 24 * 3600 * 1000)
    val sdf = new SimpleDateFormat(CommProperties.FORMAT_DATE_DAY)
    val dt = if (args.length == 0 || args(0) == null) sdf.format(date) else args(0)

    val stat = new GDKKXStatImpl(hc, dt, DateFormatUtil.STAT_CYCLE_DAY)
    val jdbc = new GDKKXStatJdbc()
    val df1 = stat.statTZ.repartition(8).persist()
    jdbc.insertTZStatList(df1, dt, DateFormatUtil.STAT_CYCLE_DAY)
    df1.unpersist()
    
    val date2 = new Date
    date2.setTime(date2.getTime - 2 * 24 * 3600 * 1000)
    val dt2 = if (args.length == 0 || args(0) == null) sdf.format(date2) else args(0)
    
    val stat2 = new GDKKXStatImpl(hc, dt2, DateFormatUtil.STAT_CYCLE_DAY)
    val df2 = stat2.statGDKKX.repartition(8).persist()
    import hc.implicits._
    jdbc.updateDGKKX(df2.filter($"SSBZ" === "SS"), dt2, DateFormatUtil.STAT_CYCLE_DAY, "SS")
    jdbc.updateDGKKX(df2.filter($"SSBZ" === "BZ"), dt2, DateFormatUtil.STAT_CYCLE_DAY, "BZ")
    df2.unpersist()
  }
}