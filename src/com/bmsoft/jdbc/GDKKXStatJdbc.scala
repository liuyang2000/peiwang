package com.bmsoft.jdbc

import java.sql.Connection
import java.sql.DriverManager
import java.sql.PreparedStatement

import org.apache.spark.sql.DataFrame

import com.bmsoft.util.DateFormatUtil
import com.bmsoft.util.JdbcConnUtil
import com.bmsoft.util.OracleUtils

/**
 * GDKKX insert into oracle
 * @author mathsyang
 */
class GDKKXStatJdbc {
  def updateDGKKX(df: DataFrame, statDate: String, statCycle: String, ssbz: String): Unit = {
    val tableName = DateFormatUtil.tableName(s"PWYW_DWGDKKXTJ_${ssbz}", statCycle)

    val tjrqParam = if (DateFormatUtil.STAT_CYCLE_WEEK.equals(statCycle)) {
      val dateFrom = DateFormatUtil.dateFrom(statDate, statCycle)
      s"tjksrq = to_date('${dateFrom}','yyyymmdd')"
    } else if (DateFormatUtil.STAT_CYCLE_DAY.equals(statCycle))
      s"tjrq = to_date('${statDate}','yyyymmdd')"
    else
      s"tjrq = '${statDate}'"

    val sql =
      s"""UPDATE ${tableName} SET 
            DQGDKKX = ?,
            GDKKX = ?,
            XR_SJ = sysdate
            WHERE DWBM = ? and CNW = ? and ${tjrqParam}"""

    df.foreachPartition(row => {
      var conn: Connection = null
      var ps: PreparedStatement = null
      try {
        Class.forName(JdbcConnUtil.driver)
        conn = DriverManager.getConnection(JdbcConnUtil.url, JdbcConnUtil.userName, JdbcConnUtil.passwd)
        ps = conn.prepareStatement(sql)
        conn.setAutoCommit(false)
        var iCnt: Int = 0
        row.foreach(data => {
          iCnt += 1
          var index = 1
          OracleUtils.ExecPS(data, ps, index, "DQGDKKX", java.sql.Types.NUMERIC); index += 1
          OracleUtils.ExecPS(data, ps, index, "GDKKX", java.sql.Types.NUMERIC); index += 1
          OracleUtils.ExecPS(data, ps, index, "DWBM", java.sql.Types.VARCHAR); index += 1
          OracleUtils.ExecPS(data, ps, index, "CNW", java.sql.Types.VARCHAR)
          ps.addBatch()

          if (iCnt % JdbcConnUtil.BATCH_COMMIT_CNT == 0 && iCnt != 0) {
            ps.executeBatch()
            ps.clearBatch()
            conn.commit()
          }
        })
        ps.executeBatch()
        ps.clearBatch()
        conn.commit()
//      } catch {
//        case e: Exception => e.printStackTrace()
      } finally {
        if (ps != null) {
          ps.close()
        }
        if (conn != null) {
          conn.close()
        }
      }
    })
  }

  def insertTZStatList(df: DataFrame, statDate: String, statCycle: String): Unit = {
    val tableName = DateFormatUtil.tableName(s"PWYW_DWTZTJ", statCycle)

    OracleUtils.detCurDayData(tableName, statCycle, statDate)

    val tjrq = if (DateFormatUtil.STAT_CYCLE_WEEK.equals(statCycle)) "TJKSRQ,TJJSRQ" else "TJRQ"

    val dateFrom = DateFormatUtil.dateFrom(statDate, statCycle)
    val dateTo = DateFormatUtil.dateTo(dateFrom, statCycle)
    val weekDateTo = DateFormatUtil.dateDay(dateTo, 1)
    val tjrqParam = if (DateFormatUtil.STAT_CYCLE_WEEK.equals(statCycle))
      s"to_date('${dateFrom}','yyyymmdd'),to_date('${weekDateTo}','yyyymmdd')"
    else if (DateFormatUtil.STAT_CYCLE_DAY.equals(statCycle))
      s"to_date('${statDate}','yyyymmdd')"
    else
      s"'${statDate}'"

    val sql = s"INSERT INTO ${tableName}\n" +
      s"(${tjrq},DWBM,CNW,DWMC,DWJB,SJDWBM,SJDWMC,TZKGSL,TZCS,BGL_TZCS,ZX_TZCS,ZHIX_TZCS,FZX_TZCS,FDX_TZCS,KX_TZCS,LJ_TZCS,TZKGSL_TB,TZKGSL_HB,TZCS_TB,TZCS_HB,BGL_TZCS_TB,BGL_TZCS_HB)\n" +
      "VALUES\n" +
      s"(${tjrqParam},?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) " //log errors into ${errTableName}('TEST')\n"

    df.foreachPartition(row => {
      var conn: Connection = null
      var ps: PreparedStatement = null
      try {
        Class.forName(JdbcConnUtil.driver)
        conn = DriverManager.getConnection(JdbcConnUtil.url, JdbcConnUtil.userName, JdbcConnUtil.passwd)
        ps = conn.prepareStatement(sql)
        conn.setAutoCommit(false)
        var iCnt: Int = 0
        row.foreach(data => {
          iCnt += 1
          var index = 1
          OracleUtils.ExecPS(data, ps, index, "DWBM", java.sql.Types.VARCHAR); index += 1
          OracleUtils.ExecPS(data, ps, index, "CNW", java.sql.Types.VARCHAR); index += 1
          OracleUtils.ExecPS(data, ps, index, "DWMC", java.sql.Types.VARCHAR); index += 1
          OracleUtils.ExecPS(data, ps, index, "DWJB", java.sql.Types.VARCHAR); index += 1
          OracleUtils.ExecPS(data, ps, index, "SJDWBM", java.sql.Types.VARCHAR); index += 1
          OracleUtils.ExecPS(data, ps, index, "SJDWMC", java.sql.Types.VARCHAR); index += 1
          OracleUtils.ExecPS(data, ps, index, "TZKGSL", java.sql.Types.NUMERIC); index += 1
          OracleUtils.ExecPS(data, ps, index, "TZCS", java.sql.Types.NUMERIC); index += 1
          OracleUtils.ExecPS(data, ps, index, "BGL_TZCS", java.sql.Types.NUMERIC); index += 1
          OracleUtils.ExecPS(data, ps, index, "ZX_TZCS", java.sql.Types.NUMERIC); index += 1
          OracleUtils.ExecPS(data, ps, index, "ZHIX_TZCS", java.sql.Types.NUMERIC); index += 1
          OracleUtils.ExecPS(data, ps, index, "FZX_TZCS", java.sql.Types.NUMERIC); index += 1
          OracleUtils.ExecPS(data, ps, index, "FDX_TZCS", java.sql.Types.NUMERIC); index += 1
          OracleUtils.ExecPS(data, ps, index, "KX_TZCS", java.sql.Types.NUMERIC); index += 1
          OracleUtils.ExecPS(data, ps, index, "LJ_TZCS", java.sql.Types.NUMERIC); index += 1
          OracleUtils.ExecPS(data, ps, index, "TZKGSL_TB", java.sql.Types.NUMERIC); index += 1
          OracleUtils.ExecPS(data, ps, index, "TZKGSL_HB", java.sql.Types.NUMERIC); index += 1
          OracleUtils.ExecPS(data, ps, index, "TZCS_TB", java.sql.Types.NUMERIC); index += 1
          OracleUtils.ExecPS(data, ps, index, "TZCS_HB", java.sql.Types.NUMERIC); index += 1
          OracleUtils.ExecPS(data, ps, index, "BGL_TZCS_TB", java.sql.Types.NUMERIC); index += 1
          OracleUtils.ExecPS(data, ps, index, "BGL_TZCS_HB", java.sql.Types.NUMERIC)
          ps.addBatch()

          if (iCnt % JdbcConnUtil.BATCH_COMMIT_CNT == 0 && iCnt != 0) {
            ps.executeBatch()
            ps.clearBatch()
            conn.commit()
          }
        })
        ps.executeBatch()
        ps.clearBatch()
        conn.commit()
//      } catch {
//        case e: Exception => e.printStackTrace()
      } finally {
        if (ps != null) {
          ps.close()
        }
        if (conn != null) {
          conn.close()
        }
      }
    })
  }
}