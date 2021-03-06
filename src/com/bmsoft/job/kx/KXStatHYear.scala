package com.bmsoft.job

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.hive.HiveContext

import com.bmsoft.impl.KXStatImpl
import com.bmsoft.jdbc.KXStatJdbc
import com.bmsoft.util.DateFormatUtil

/**
 * 馈线统计
 *
 * @author mathsyang
 */
object KXStatHYear {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("KXStatHYear")
        .set("spark.shuffle.reduceLocality.enabled", "false")
        .set("spark.eventLog.enabled","false"))
    val hc = new HiveContext(sc)

    val dt = if (args.length == 0 || args(0) == null) DateFormatUtil.dateHY(new Date, 1)
    else if (args(0).length() == 6) args(0)
    else DateFormatUtil.dateHY(args(0), 0)

    val stat = new KXStatImpl(hc, dt, DateFormatUtil.STAT_CYCLE_HYEAR)
    val jdbc = new KXStatJdbc()
    val df1 = stat.statRunStatus.repartition(8).persist()
    jdbc.insertRunStat(df1, dt, DateFormatUtil.STAT_CYCLE_HYEAR)
    df1.unpersist()
    val df2 = stat.statExcept.repartition(8).persist()
    jdbc.insertKXExcept(df2, dt, DateFormatUtil.STAT_CYCLE_HYEAR)
    df2.unpersist()
    val df3 = stat.statSSExcept.repartition(8).persist()
    jdbc.insertSSKXExcept(df3, dt, DateFormatUtil.STAT_CYCLE_HYEAR)
    df3.unpersist()
    val df4 = stat.statSSGatherSuccRate.repartition(8).persist()
    jdbc.insertKXSSGatherSucc(df4, dt, DateFormatUtil.STAT_CYCLE_HYEAR)
    df4.unpersist()
    val df5 = stat.statBZExcept.repartition(8).persist()
    jdbc.insertBZKXExcept(df5, dt, DateFormatUtil.STAT_CYCLE_HYEAR)
    df5.unpersist()
    val df6 = stat.statBZGatherSuccRate.repartition(8).persist()
    jdbc.insertKXBZGatherSucc(df6, dt, DateFormatUtil.STAT_CYCLE_HYEAR)
    df6.unpersist()
    val df7 = stat.statSSClass.repartition(8).persist()
    jdbc.insertKXSSClass(df7, dt, DateFormatUtil.STAT_CYCLE_HYEAR)
    df7.unpersist()
  }
}