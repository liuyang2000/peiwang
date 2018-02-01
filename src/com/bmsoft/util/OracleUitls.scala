package com.bmsoft.util

import java.sql.PreparedStatement
import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.sql.Row

import java.sql.DriverManager
import java.sql.Connection

object OracleUtils {
  def buildSchema3(): Unit = {
    val str = """|-- DWBM: string (nullable = true)
 |-- CNW: string (nullable = false)
 |-- DWMC: string (nullable = true)
 |-- DWJB: string (nullable = true)
 |-- SJDWBM: string (nullable = true)
 |-- SJDWMC: string (nullable = true)
 |-- TZKGSL: long (nullable = false)
 |-- TZCS: long (nullable = false)
 |-- BGL_TZCS: double (nullable = true)
 |-- ZX_TZCS: long (nullable = true)
 |-- ZHIX_TZCS: long (nullable = true)
 |-- FZX_TZCS: long (nullable = true)
 |-- FDX_TZCS: long (nullable = true)
 |-- KX_TZCS: long (nullable = true)
 |-- LJ_TZCS: decimal(21,0) (nullable = true)
 |-- TZKGSL_TB: decimal(21,0) (nullable = true)
 |-- TZKGSL_HB: decimal(21,0) (nullable = true)
 |-- TZCS_TB: decimal(21,0) (nullable = true)
 |-- TZCS_HB: decimal(21,0) (nullable = true)
 |-- BGL_TZCS_TB: decimal(21,0) (nullable = true)
 |-- BGL_TZCS_HB: decimal(21,0) (nullable = true)
 |-- DWBM: string (nullable = true)
 |-- CNW: string (nullable = true)"""

    val str1 = str.replaceAll("\\|--", "").replaceAll("\\(nullable = false\\)", "").replaceAll("\\(nullable = true\\)", "").replaceAll(" ", "")
    val result = str1.split(":").mkString(" ")
    val arry = result.split("\n")
    val result1 = arry.zipWithIndex.map {
      r =>
        {
          val row = r._1.split(" ")
          if (row(1) == "string")
            s"""OracleUtils.ExecPS(data, ps, index, "${row(0)}", java.sql.Types.VARCHAR); index += 1"""
          else if (row(1) == "timestamp")
            s"""OracleUtils.ExecPS(data, ps, index, "${row(0)}", java.sql.Types.TIMESTAMP); index += 1"""
          else
            s"""OracleUtils.ExecPS(data, ps, index, "${row(0)}", java.sql.Types.NUMERIC); index += 1"""
        }
    }
    println(arry.zipWithIndex.map(r => r._2 + " " + r._1).mkString("\n"))
    println(result1.mkString("\n"))
    println(arry.size)
    println((1 to arry.size).map(_ => "?").mkString(","))
    println(arry.map(_.replaceAll(" string", "").replaceAll(" float", "").replaceAll(" integer", "").replaceAll(" timestamp", "").replaceAll(" decimal(\\S+)", "").replaceAll(" long", "").replaceAll(" double", "")).mkString(","))
  }

  def StringToDate(s: Any): java.sql.Date = {
    val sdf = new SimpleDateFormat(CommProperties.FORMAT_DATE_DAY)
    try {
      val date = s.asInstanceOf[String]
      new java.sql.Date(sdf.parse(date).getTime)
    } catch {
      case e: Exception => null
    }
  }

  def StringToTimeStamp(s: Any): java.sql.Timestamp = {
    val sdf = new SimpleDateFormat(CommProperties.FORMAT_TIMESTAMP)
    try {
      val date = s.asInstanceOf[String]
      java.sql.Timestamp.valueOf(date)
    } catch {
      case e: Exception => null
    }
  }

  def ExecPS(row: Row, ps: PreparedStatement, index: Int, filedName: String, sqlType: Int): Unit = {
    val value = row.getAs[Any](filedName)
    val ind = row.fieldIndex(filedName)
    if (row.isNullAt(ind))
      ps.setNull(index, sqlType)
    else {
      if (value.isInstanceOf[String] && sqlType == 91) {
        if (StringToDate(value) != null) ps.setDate(index, StringToDate(value))
        else ps.setNull(index, sqlType)
      } else if (value.isInstanceOf[String] && sqlType == 93) {
        if (StringToTimeStamp(value) != null) ps.setTimestamp(index, StringToTimeStamp(value))
        else ps.setNull(index, sqlType)
      } else {
        value match {
          case value: java.lang.Long => ps.setLong(index, value.asInstanceOf[java.lang.Long])
          case value: java.lang.Double => if (value.isNaN()) ps.setNull(index, sqlType) else ps.setDouble(index, value.asInstanceOf[java.lang.Double])
          case value: java.lang.Integer => ps.setInt(index, value.asInstanceOf[Int])
          case value: java.lang.Float => if (value.isNaN()) ps.setNull(index, sqlType) else ps.setFloat(index, value.asInstanceOf[java.lang.Float])
          case value: java.math.BigDecimal => ps.setBigDecimal(index, value.asInstanceOf[java.math.BigDecimal])
          case value: java.sql.Date => ps.setDate(index, value.asInstanceOf[java.sql.Date])
          case value: java.sql.Timestamp => ps.setTimestamp(index, value.asInstanceOf[java.sql.Timestamp])
          case value: String => ps.setString(index, value.asInstanceOf[String])
        }
      }
    }
  }

  def insertTotalCnw(sql: String) {
    var conn: Connection = null
    var ps: PreparedStatement = null
    try {
      Class.forName(JdbcConnUtil.driver)
      conn = DriverManager.getConnection(JdbcConnUtil.url, JdbcConnUtil.userName, JdbcConnUtil.passwd)
      ps = conn.prepareStatement(sql)

      ps.execute()
      //    } catch {
      //      case e: Exception => e.printStackTrace()
    } finally {
      if (ps != null) {
        ps.close()
      }
      if (conn != null) {
        conn.close()
      }
    }
  }

  def detCurDayData(tableName: String, statCycle: String, statDate: String): Unit = {
    val delTjrq = if (DateFormatUtil.STAT_CYCLE_WEEK.equals(statCycle)) "TJKSRQ" else "TJRQ"

    val tjrq = if (DateFormatUtil.STAT_CYCLE_WEEK.equals(statCycle)) DateFormatUtil.dateFrom(statDate, DateFormatUtil.STAT_CYCLE_WEEK) else statDate
    val delSql = if (DateFormatUtil.STAT_CYCLE_DAY.equals(statCycle) || DateFormatUtil.STAT_CYCLE_WEEK.equals(statCycle))
      s"delete from ${tableName} where ${delTjrq} = to_date('${tjrq}','yyyymmdd')"
    else
      s"delete from ${tableName} where ${delTjrq} = '${statDate}'"

    var conn: Connection = null
    var ps: PreparedStatement = null

    try {
      Class.forName(JdbcConnUtil.driver)
      conn = DriverManager.getConnection(JdbcConnUtil.url, JdbcConnUtil.userName, JdbcConnUtil.passwd)
      ps = conn.prepareStatement(delSql)
      ps.execute()
      //    } catch {
      //      case e: Exception => e.printStackTrace()
    } finally {
      if (ps != null) {
        ps.close()
      }
      if (conn != null) {
        conn.close()
      }
    }
  }

  def main(args: Array[String]): Unit = {
    OracleUtils.buildSchema3
  }
}