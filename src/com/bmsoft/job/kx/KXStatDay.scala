package com.bmsoft.job

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.hive.HiveContext

import com.bmsoft.impl.KXStatImpl
import com.bmsoft.jdbc.KXStatJdbc
import com.bmsoft.util.DateFormatUtil
import com.bmsoft.util.CommProperties

/**
 * 馈线统计
 *
 * @author mathsyang
 */
object KXStatDay {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("KXStatDay")
        .set("spark.shuffle.reduceLocality.enabled", "false")
        .set("spark.eventLog.enabled","false"))
    val hc = new HiveContext(sc)

    val date = new Date
    date.setTime(date.getTime - 24 * 3600 * 1000 * 2)
    val sdf = new SimpleDateFormat(CommProperties.FORMAT_DATE_DAY)
    val dt = if (args.length == 0 || args(0) == null) sdf.format(date) else args(0)

    val stat = new KXStatImpl(hc, dt, DateFormatUtil.STAT_CYCLE_DAY)
    val jdbc = new KXStatJdbc()
    val df1 = stat.statRunStatus.repartition(8).persist()
    jdbc.insertRunStat(df1, dt, DateFormatUtil.STAT_CYCLE_DAY)
    df1.unpersist()
    val df2 = stat.statExcept.repartition(8).persist()
    jdbc.insertKXExcept(df2, dt, DateFormatUtil.STAT_CYCLE_DAY)
    df2.unpersist()
    val df3 = stat.statSSExcept.repartition(8).persist()
    jdbc.insertSSKXExcept(df3, dt, DateFormatUtil.STAT_CYCLE_DAY)
    df3.unpersist()
    val df4 = stat.statSSGatherSuccRate.repartition(8).persist()
    jdbc.insertKXSSGatherSucc(df4, dt, DateFormatUtil.STAT_CYCLE_DAY)
    df4.unpersist()
    val df5 = stat.statBZExcept.repartition(8).persist()
    jdbc.insertBZKXExcept(df5, dt, DateFormatUtil.STAT_CYCLE_DAY)
    df5.unpersist()
    val df6 = stat.statBZGatherSuccRate.repartition(8).persist()
    jdbc.insertKXBZGatherSucc(df6, dt, DateFormatUtil.STAT_CYCLE_DAY)
    df6.unpersist()
    val df7 = stat.statSSClass.repartition(8).persist()
    jdbc.insertKXSSClass(df7, dt, DateFormatUtil.STAT_CYCLE_DAY)
    df7.unpersist()
  }
}