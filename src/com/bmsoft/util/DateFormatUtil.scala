package com.bmsoft.util

import java.util.Date
import java.util.Calendar
import java.text.SimpleDateFormat

object DateFormatUtil {
  val STAT_CYCLE_DAY = "DAY"
  val STAT_CYCLE_WEEK = "WEEK"
  val STAT_CYCLE_MON = "MON"
  val STAT_CYCLE_SEASON = "SEASON"
  val STAT_CYCLE_HYEAR = "HYEAR"
  val STAT_CYCLE_YEAR = "YEAR"
  val sdf = new SimpleDateFormat(CommProperties.FORMAT_DATE_DAY)

  val dateCycle = -2

  def getCycleDate() : Date={
    val cal = Calendar.getInstance()
    cal.add(Calendar.DAY_OF_YEAR, DateFormatUtil.dateCycle)
    cal.getTime
  }
  
  def statHours(statDate: String, statCycle: String): Long = {
    val preStatDate = datePre(statDate, statCycle)
    
    val dateFromD = sdf.parse(statDate)
    val preDateFromD = sdf.parse(dateFrom(preStatDate, statCycle))
    
    (dateFromD.getTime - preDateFromD.getTime) / (1000 * 3600)
  }
  
  def yearToNowHours(statDate: String): Long = {
    val year = yearOfDate(statDate)
    
    val dateFromD = sdf.parse(statDate)
    val preDateFromD = sdf.parse(year + "0101")
    
    (dateFromD.getTime - preDateFromD.getTime) / (1000 * 3600)
  }
  
  def preYear(statDate: String): String = {
    val preYear = dateYear(statDate, 1)
    preYear + statDate.substring(4)
  }
  
  def yearOfDate(statDate: String): String = {
    if(statDate != null && statDate.length() >= 4) statDate.substring(0,4) else null
  }
  
  def datePre(statDate: String, statCycle: String): String = {
    statCycle match {
      case STAT_CYCLE_DAY    => dateDay(statDate, 1)
      case STAT_CYCLE_WEEK   => dateWeek(statDate, 1)
      case STAT_CYCLE_MON    => dateMon(statDate, 1)
      case STAT_CYCLE_SEASON => dateSeason(statDate, 1)
      case STAT_CYCLE_HYEAR  => dateHY(statDate, 1)
      case STAT_CYCLE_YEAR   => dateYear(statDate, 1)
      case _                 => ""
    }
  }
  def dateDay(dateStr: String, cycle: Int): String = {
    val date = sdf.parse(dateStr)
    dateDay(date, cycle)
  }
  def dateDay(date: Date, cycle: Int): String = {
    val cal = Calendar.getInstance()
    cal.setTime(date)
    cal.add(Calendar.DAY_OF_YEAR, -cycle)

    sdf.format(cal.getTime)
  }

  /**
   * 给定一个日期date，返回其指定cycle周期前的年份+季度字符串
   * 例如给定date=20161129 cycle=1， 返回20163
   *
   * @param date
   * @param cycle
   * @return
   */
  def dateSeason(dateStr: String, cycle: Int): String = {
    val date = sdf.parse(dateStr)
    dateSeason(date, cycle)
  }
  def dateSeason(date: Date, cycle: Int): String = {
    val cal = Calendar.getInstance()
    cal.setTime(date)
    cal.add(Calendar.MONTH, -cycle * 3)
    val mon = cal.get(Calendar.MONTH)
    var season = "04"
    if (mon < 3) season = "01"
    else if (mon < 6) season = "02"
    else if (mon < 9) season = "03"

    cal.get(Calendar.YEAR) + season
  }

  /**
   * 给定一个日期date，返回其指定cycle周期前的年份+上下半年标志（上半年为1，下半年为2）字符串
   * 例如给定date=20161129 cycle=1， 返回20161
   *
   * @param date
   * @param cycle
   * @return
   */
  def dateHY(dateStr: String, cycle: Int): String = {
    val date = sdf.parse(dateStr)
    dateHY(date, cycle)
  }
  def dateHY(date: Date, cycle: Int): String = {
    val cal = Calendar.getInstance()
    cal.setTime(date)
    cal.add(Calendar.MONTH, -cycle * 6)
    val mon = cal.get(Calendar.MONTH)
    var harf = "02"
    if (mon < 6) harf = "01"

    cal.get(Calendar.YEAR) + harf
  }

  /**
   * 给定一个日期date，返回其指定cycle周期前的年份+月份字符串
   * 例如给定date=20161129 cycle=1， 返回201610
   *
   * @param date
   * @param cycle
   * @return
   */
  def dateMon(dateStr: String, cycle: Int): String = {
    val date = sdf.parse(dateStr)
    dateMon(date, cycle)
  }
  def dateMon(date: Date, cycle: Int): String = {
    val cal = Calendar.getInstance()
    cal.setTime(date)
    cal.add(Calendar.MONTH, -cycle)
    val mon = cal.get(Calendar.MONTH) + 1

    var monStr = mon + ""
    if (mon < 10) monStr = "0" + monStr
    cal.get(Calendar.YEAR) + monStr
  }

  /**
   * 给定一个日期date，返回其指定cycle周期前的年份+周数字符串
   * 例如给定date=20161129 cycle=1， 返回201648
   *
   * @param date
   * @param cycle
   * @return
   */
  def dateWeek(dateStr: String, cycle: Int): String = {
    val date = sdf.parse(dateStr)
    dateWeek(date, cycle)
  }
  def dateWeek(date: Date, cycle: Int): String = {
    val cal = Calendar.getInstance()
    cal.setTime(date)
    cal.add(Calendar.DAY_OF_YEAR, -7 * cycle)
    val week = cal.get(Calendar.WEEK_OF_YEAR)

    var weekStr = week + ""
    if (week < 10) weekStr = "0" + weekStr
    cal.get(Calendar.YEAR) + weekStr
  }

  /**
   * 给定一个日期date，返回其指定cycle周期前的年份字符串
   * 例如给定date=20161129 cycle=1， 返回201648
   *
   * @param date
   * @param cycle
   * @return
   */
  def dateYear(dateStr: String, cycle: Int): String = {
    val date = sdf.parse(dateStr)
    dateYear(date, cycle)
  }
  def dateYear(date: Date, cycle: Int): String = {
    val cal = Calendar.getInstance()
    cal.setTime(date)
    cal.add(Calendar.YEAR, -cycle)
    cal.get(Calendar.YEAR).toString()
  }

  def dateFrom(statDate: String, statCycle: String): String = {
    val dateFrom =
      if (STAT_CYCLE_DAY.equals(statCycle)) statDate
      else if (STAT_CYCLE_WEEK.equals(statCycle)) {
        val year = statDate.substring(0, 4)
        val week = statDate.substring(4)

        val cal = Calendar.getInstance()
        cal.set(Calendar.YEAR, year.toInt)
        cal.set(Calendar.WEEK_OF_YEAR, week.toInt)
        cal.set(Calendar.DAY_OF_WEEK, 2)

        val sdf = new SimpleDateFormat("yyyyMMdd")
        val df = sdf.format(cal.getTime)
        df
      } else if (STAT_CYCLE_MON.equals(statCycle)) {
        statDate + "01"
      } else if (STAT_CYCLE_SEASON.equals(statCycle)) {
        val year = statDate.substring(0, 4)
        val season = statDate.substring(4).toInt - 1
        val mon = if (season < 3) "0" + (season * 3 + 1) else "" + (season * 3 + 1)
        year + mon + "01"
      } else if (STAT_CYCLE_HYEAR.equals(statCycle)) {
        val year = statDate.substring(0, 4)
        val h = statDate.substring(4).toInt
        val mon = if (h == 1) "01" else "07"

        year + mon + "01"
      } else {
        statDate.substring(0, 4) + "0101"
      }

    dateFrom
  }

  def dateTo(dateFrom: String, statCycle: String): String = {
    val cal = Calendar.getInstance()
    val sdf = new SimpleDateFormat("yyyyMMdd")
    cal.setTime(sdf.parse(dateFrom))
    val dateTo =
      if (STAT_CYCLE_DAY.equals(statCycle)) {
        cal.add(Calendar.DAY_OF_YEAR, 1)
        sdf.format(cal.getTime)
      } else if (STAT_CYCLE_WEEK.equals(statCycle)) {
        cal.add(Calendar.DAY_OF_YEAR, 7)
        sdf.format(cal.getTime)
      } else if (STAT_CYCLE_MON.equals(statCycle)) {
        cal.add(Calendar.MONTH, 1)
        sdf.format(cal.getTime)
      } else if (STAT_CYCLE_SEASON.equals(statCycle)) {
        cal.add(Calendar.MONTH, 3)
        sdf.format(cal.getTime)
      } else if (STAT_CYCLE_HYEAR.equals(statCycle)) {
        cal.add(Calendar.MONTH, 6)
        sdf.format(cal.getTime)
      } else {
        cal.add(Calendar.MONTH, 12)
        sdf.format(cal.getTime)
      }
    dateTo
  }

  def statDays(dateFrom: String, dateTo: String, statCycle: String): Long = {
    val days =
      if (STAT_CYCLE_DAY.equals(statCycle)) 1l
      else if (STAT_CYCLE_WEEK.equals(statCycle)) 7l
      else {
        val sdf = new SimpleDateFormat("yyyyMMdd")
        val df = sdf.parse(dateFrom)
        val dt = sdf.parse(dateTo)
        val differDay = (dt.getTime - df.getTime) / (3600 * 24 * 1000)
        differDay
      }
    days
  }

  def tableName(tablePrefix: String, statCycle: String): String = {
    val tn =
      if (STAT_CYCLE_DAY.equals(statCycle)) tablePrefix + "_R"
      else if (STAT_CYCLE_WEEK.equals(statCycle)) tablePrefix + "_Z"
      else if (STAT_CYCLE_MON.equals(statCycle)) tablePrefix + "_Y"
      else if (STAT_CYCLE_SEASON.equals(statCycle)) tablePrefix + "_J"
      else if (STAT_CYCLE_HYEAR.equals(statCycle)) tablePrefix + "_BN"
      else if (STAT_CYCLE_YEAR.equals(statCycle)) tablePrefix + "_N"
      else ""
    tn
  }
}