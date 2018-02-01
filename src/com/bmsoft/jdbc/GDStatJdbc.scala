package com.bmsoft.jdbc

import java.sql.Connection
import java.sql.DriverManager
import java.sql.PreparedStatement

import org.apache.spark.sql.DataFrame

import com.bmsoft.util.DateFormatUtil
import com.bmsoft.util.JdbcConnUtil
import com.bmsoft.util.OracleUtils

/**
 * GD insert into oracle
 * @author mathsyang
 */
class GDStatJdbc {
  def insertGDStatList(df: DataFrame, statDate: String, statCycle: String): Unit = {
    val tableName = DateFormatUtil.tableName("PWYW_GDFLTJ_SSXBZ", statCycle)
    val errTableName = DateFormatUtil.tableName("ERR_PWYW_GDTJ_SSXBZ", statCycle)

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
      s"(${tjrq},DWBM,DQHJ,GZYYFL,JDFS,SFWFD,FDSC,GDSL,YXGBSL,YXZBSL,YXYHSL,QXZSC_QXC,QXPJSC_QXC,QXZSC_YHC,QXPJSC_YHC,DWMC,DWJB,SJDWBM,SJDWMC)\n" +
      "VALUES\n" +
      s"(${tjrqParam},?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) " //log errors into ${errTableName}('TEST')\n"

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
          OracleUtils.ExecPS(data, ps, index, "DQHJ", java.sql.Types.VARCHAR); index += 1
          OracleUtils.ExecPS(data, ps, index, "GZYYFL", java.sql.Types.VARCHAR); index += 1
          OracleUtils.ExecPS(data, ps, index, "JDFS", java.sql.Types.VARCHAR); index += 1
          OracleUtils.ExecPS(data, ps, index, "SFWFD", java.sql.Types.VARCHAR); index += 1
          OracleUtils.ExecPS(data, ps, index, "FDSC", java.sql.Types.VARCHAR); index += 1
          OracleUtils.ExecPS(data, ps, index, "GDSL", java.sql.Types.VARCHAR); index += 1
          OracleUtils.ExecPS(data, ps, index, "YXGBSL", java.sql.Types.VARCHAR); index += 1
          OracleUtils.ExecPS(data, ps, index, "YXZBSL", java.sql.Types.VARCHAR); index += 1
          OracleUtils.ExecPS(data, ps, index, "YXYHSL", java.sql.Types.VARCHAR); index += 1
          OracleUtils.ExecPS(data, ps, index, "QXZSC_QXC", java.sql.Types.NUMERIC); index += 1
          OracleUtils.ExecPS(data, ps, index, "QXPJSC_QXC", java.sql.Types.NUMERIC); index += 1
          OracleUtils.ExecPS(data, ps, index, "QXZSC_YHC", java.sql.Types.NUMERIC); index += 1
          OracleUtils.ExecPS(data, ps, index, "QXPJSC_YHC", java.sql.Types.NUMERIC); index += 1
          OracleUtils.ExecPS(data, ps, index, "DWMC", java.sql.Types.VARCHAR); index += 1
          OracleUtils.ExecPS(data, ps, index, "DWJB", java.sql.Types.VARCHAR); index += 1
          OracleUtils.ExecPS(data, ps, index, "SJDWBM", java.sql.Types.VARCHAR); index += 1
          OracleUtils.ExecPS(data, ps, index, "SJDWMC", java.sql.Types.VARCHAR)
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