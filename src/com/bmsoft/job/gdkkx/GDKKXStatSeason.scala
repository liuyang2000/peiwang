package com.bmsoft.job

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.hive.HiveContext

import com.bmsoft.util.DateFormatUtil
import com.bmsoft.impl.GDKKXStatImpl
import com.bmsoft.jdbc.GDKKXStatJdbc

/**
 * 馈线统计
 *
 * @author mathsyang
 */
object GDKKXStatSeason {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("GDKKXStatSeason")
      .set("spark.shuffle.reduceLocality.enabled", "false")
      .set("spark.eventLog.enabled", "false"))
    val hc = new HiveContext(sc)

    val dt = if (args.length == 0 || args(0) == null) DateFormatUtil.dateSeason(new Date, 1)
    else if (args(0).length() == 6) args(0)
    else DateFormatUtil.dateSeason(args(0), 0)

    val stat = new GDKKXStatImpl(hc, dt, DateFormatUtil.STAT_CYCLE_SEASON)
    val jdbc = new GDKKXStatJdbc()
    val df1 = stat.statTZ.repartition(8).persist()
    jdbc.insertTZStatList(df1, dt, DateFormatUtil.STAT_CYCLE_SEASON)
    df1.unpersist()
    
    val df2 = stat.statGDKKX.repartition(8).persist()
    import hc.implicits._
    jdbc.updateDGKKX(df2.filter($"SSBZ" === "SS"), dt, DateFormatUtil.STAT_CYCLE_SEASON, "SS")
    jdbc.updateDGKKX(df2.filter($"SSBZ" === "BZ"), dt, DateFormatUtil.STAT_CYCLE_SEASON, "BZ")
    df2.unpersist()
  }
}