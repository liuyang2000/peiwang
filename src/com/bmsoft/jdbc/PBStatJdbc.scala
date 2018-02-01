package com.bmsoft.jdbc

import java.sql.Connection
import java.sql.DriverManager
import java.sql.PreparedStatement

import org.apache.spark.sql.DataFrame

import com.bmsoft.util.DateFormatUtil
import com.bmsoft.util.JdbcConnUtil
import com.bmsoft.util.OracleUtils

/**
 * PB insert into oracle
 * @author mathsyang
 */
class PBStatJdbc {
  def queryCondition(statDate: String, statCycle: String): String = {
    val dateFrom = DateFormatUtil.dateFrom(statDate, statCycle)
    val condition = if (DateFormatUtil.STAT_CYCLE_WEEK.equals(statCycle)) s"tjksrq = to_date(${dateFrom}, 'yyyymmdd')"
    else if (DateFormatUtil.STAT_CYCLE_DAY.equals(statCycle)) s"tjrq = to_date(${statDate}, 'yyyymmdd')"
    else s"tjrq = '${statDate}'"

    condition
  }

  def insertRunStat(df: DataFrame, statDate: String, statCycle: String): Unit = {
    val tableName = DateFormatUtil.tableName("PWYW_PBYXZTTJ", statCycle)
    val errTableName = DateFormatUtil.tableName("ERR_PWYW_PBYXZTTJ", statCycle)

    OracleUtils.detCurDayData(tableName, statCycle, statDate)

    val tjrq = if (DateFormatUtil.STAT_CYCLE_WEEK.equals(statCycle)) "TJKSRQ,TJJSRQ" else "TJRQ"
    val tjrqParam = if (DateFormatUtil.STAT_CYCLE_WEEK.equals(statCycle)) "?,?" else "?"
    val sql = s"INSERT INTO ${tableName}\n" +
      s"(PBID,PBMC,ZXMC,SSXL,XLMC,SSGT,GTMC,DWBM,DWMC,DWJB,SJDWBM,SJDWMC,BZID,DYDJ,SBZT,TYRQ,CNW,SFDW,ZYCD,PBLX,YHFL,YHJLFS,YDDZ,EDRL,CT,PT,ZHBL,YXZT,${tjrq},ZDYGGL,ZDYGGL_SJ,ZXYGGL,ZXYGGL_SJ,ZDSZGL,ZDSZGL_SJ,ZXGLYS,ZXGLYS_SJ,ZDDL_A,ZDDL_A_SJ,ZXDL_A,ZXDL_A_SJ,ZDDL_B,ZDDL_B_SJ,ZXDL_B,ZXDL_B_SJ,ZDDL_C,ZDDL_C_SJ,ZXDL_C,ZXDL_C_SJ,ZXDY_A,ZXDY_A_SJ,ZXDY_B,ZXDY_B_SJ,ZXDY_C,ZXDY_C_SJ,ZDDY_A,ZDDY_A_SJ,ZDDY_B,ZDDY_B_SJ,ZDDY_C,ZDDY_C_SJ,PJGL,FHL,FZL,AXDYQXYCDS,AXDYQXSCDS,BXDYQXYCDS,BXDYQXSCDS,CXDYQXYCDS,CXDYQXSCDS,AXDLQXYCDS,AXDLQXSCDS,BXDLQXYCDS,BXDLQXSCDS,CXDLQXYCDS,CXDLQXSCDS,GLYSQXYCDS,GLYSQXSCDS,YGZGLQXYCDS,YGZGLQXSCDS,WGZGLQXYCDS,WGZGLQXSCDS)\n" +
      "VALUES\n" +
      s"(${tjrqParam},?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) " //log errors into ${errTableName}('TEST')\n"

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
          OracleUtils.ExecPS(data, ps, index, "PBID", java.sql.Types.VARCHAR); index += 1
          OracleUtils.ExecPS(data, ps, index, "PBMC", java.sql.Types.VARCHAR); index += 1
          OracleUtils.ExecPS(data, ps, index, "ZXMC", java.sql.Types.VARCHAR); index += 1
          OracleUtils.ExecPS(data, ps, index, "SSXL", java.sql.Types.VARCHAR); index += 1
          OracleUtils.ExecPS(data, ps, index, "XLMC", java.sql.Types.VARCHAR); index += 1
          OracleUtils.ExecPS(data, ps, index, "SSGT", java.sql.Types.VARCHAR); index += 1
          OracleUtils.ExecPS(data, ps, index, "GTMC", java.sql.Types.VARCHAR); index += 1
          OracleUtils.ExecPS(data, ps, index, "DWBM", java.sql.Types.VARCHAR); index += 1
          OracleUtils.ExecPS(data, ps, index, "DWMC", java.sql.Types.VARCHAR); index += 1
          OracleUtils.ExecPS(data, ps, index, "DWJB", java.sql.Types.VARCHAR); index += 1
          OracleUtils.ExecPS(data, ps, index, "SJDWBM", java.sql.Types.VARCHAR); index += 1
          OracleUtils.ExecPS(data, ps, index, "SJDWMC", java.sql.Types.VARCHAR); index += 1
          OracleUtils.ExecPS(data, ps, index, "BZID", java.sql.Types.VARCHAR); index += 1
          OracleUtils.ExecPS(data, ps, index, "DYDJ", java.sql.Types.VARCHAR); index += 1
          OracleUtils.ExecPS(data, ps, index, "SBZT", java.sql.Types.VARCHAR); index += 1
          OracleUtils.ExecPS(data, ps, index, "TYRQ", java.sql.Types.TIMESTAMP); index += 1
          OracleUtils.ExecPS(data, ps, index, "CNW", java.sql.Types.VARCHAR); index += 1
          OracleUtils.ExecPS(data, ps, index, "SFDW", java.sql.Types.VARCHAR); index += 1
          OracleUtils.ExecPS(data, ps, index, "ZYCD", java.sql.Types.VARCHAR); index += 1
          OracleUtils.ExecPS(data, ps, index, "PBLX", java.sql.Types.VARCHAR); index += 1
          OracleUtils.ExecPS(data, ps, index, "YHFL", java.sql.Types.VARCHAR); index += 1
          OracleUtils.ExecPS(data, ps, index, "YHJLFS", java.sql.Types.NUMERIC); index += 1
          OracleUtils.ExecPS(data, ps, index, "YDDZ", java.sql.Types.VARCHAR); index += 1
          OracleUtils.ExecPS(data, ps, index, "EDRL", java.sql.Types.NUMERIC); index += 1
          OracleUtils.ExecPS(data, ps, index, "CT", java.sql.Types.NUMERIC); index += 1
          OracleUtils.ExecPS(data, ps, index, "PT", java.sql.Types.NUMERIC); index += 1
          OracleUtils.ExecPS(data, ps, index, "ZHBL", java.sql.Types.NUMERIC); index += 1
          OracleUtils.ExecPS(data, ps, index, "YXZT", java.sql.Types.VARCHAR); index += 1
          if (DateFormatUtil.STAT_CYCLE_WEEK.equals(statCycle)) {
            OracleUtils.ExecPS(data, ps, index, "TJKSRQ", java.sql.Types.DATE); index += 1
            OracleUtils.ExecPS(data, ps, index, "TJJSRQ", java.sql.Types.DATE); index += 1
          } else if (DateFormatUtil.STAT_CYCLE_DAY.equals(statCycle)) {
            OracleUtils.ExecPS(data, ps, index, "TJRQ", java.sql.Types.DATE); index += 1
          } else {
            OracleUtils.ExecPS(data, ps, index, "TJRQ", java.sql.Types.VARCHAR); index += 1
          }
          OracleUtils.ExecPS(data, ps, index, "ZDYGGL", java.sql.Types.NUMERIC); index += 1
          OracleUtils.ExecPS(data, ps, index, "ZDYGGL_SJ", java.sql.Types.TIMESTAMP); index += 1
          OracleUtils.ExecPS(data, ps, index, "ZXYGGL", java.sql.Types.NUMERIC); index += 1
          OracleUtils.ExecPS(data, ps, index, "ZXYGGL_SJ", java.sql.Types.TIMESTAMP); index += 1
          OracleUtils.ExecPS(data, ps, index, "ZDSZGL", java.sql.Types.NUMERIC); index += 1
          OracleUtils.ExecPS(data, ps, index, "ZDSZGL_SJ", java.sql.Types.TIMESTAMP); index += 1
          OracleUtils.ExecPS(data, ps, index, "ZXGLYS", java.sql.Types.NUMERIC); index += 1
          OracleUtils.ExecPS(data, ps, index, "ZXGLYS_SJ", java.sql.Types.TIMESTAMP); index += 1
          OracleUtils.ExecPS(data, ps, index, "ZDDL_A", java.sql.Types.NUMERIC); index += 1
          OracleUtils.ExecPS(data, ps, index, "ZDDL_A_SJ", java.sql.Types.TIMESTAMP); index += 1
          OracleUtils.ExecPS(data, ps, index, "ZXDL_A", java.sql.Types.NUMERIC); index += 1
          OracleUtils.ExecPS(data, ps, index, "ZXDL_A_SJ", java.sql.Types.TIMESTAMP); index += 1
          OracleUtils.ExecPS(data, ps, index, "ZDDL_B", java.sql.Types.NUMERIC); index += 1
          OracleUtils.ExecPS(data, ps, index, "ZDDL_B_SJ", java.sql.Types.TIMESTAMP); index += 1
          OracleUtils.ExecPS(data, ps, index, "ZXDL_B", java.sql.Types.NUMERIC); index += 1
          OracleUtils.ExecPS(data, ps, index, "ZXDL_B_SJ", java.sql.Types.TIMESTAMP); index += 1
          OracleUtils.ExecPS(data, ps, index, "ZDDL_C", java.sql.Types.NUMERIC); index += 1
          OracleUtils.ExecPS(data, ps, index, "ZDDL_C_SJ", java.sql.Types.TIMESTAMP); index += 1
          OracleUtils.ExecPS(data, ps, index, "ZXDL_C", java.sql.Types.NUMERIC); index += 1
          OracleUtils.ExecPS(data, ps, index, "ZXDL_C_SJ", java.sql.Types.TIMESTAMP); index += 1
          OracleUtils.ExecPS(data, ps, index, "ZXDY_A", java.sql.Types.NUMERIC); index += 1
          OracleUtils.ExecPS(data, ps, index, "ZXDY_A_SJ", java.sql.Types.TIMESTAMP); index += 1
          OracleUtils.ExecPS(data, ps, index, "ZXDY_B", java.sql.Types.NUMERIC); index += 1
          OracleUtils.ExecPS(data, ps, index, "ZXDY_B_SJ", java.sql.Types.TIMESTAMP); index += 1
          OracleUtils.ExecPS(data, ps, index, "ZXDY_C", java.sql.Types.NUMERIC); index += 1
          OracleUtils.ExecPS(data, ps, index, "ZXDY_C_SJ", java.sql.Types.TIMESTAMP); index += 1
          OracleUtils.ExecPS(data, ps, index, "ZDDY_A", java.sql.Types.NUMERIC); index += 1
          OracleUtils.ExecPS(data, ps, index, "ZDDY_A_SJ", java.sql.Types.TIMESTAMP); index += 1
          OracleUtils.ExecPS(data, ps, index, "ZDDY_B", java.sql.Types.NUMERIC); index += 1
          OracleUtils.ExecPS(data, ps, index, "ZDDY_B_SJ", java.sql.Types.TIMESTAMP); index += 1
          OracleUtils.ExecPS(data, ps, index, "ZDDY_C", java.sql.Types.NUMERIC); index += 1
          OracleUtils.ExecPS(data, ps, index, "ZDDY_C_SJ", java.sql.Types.TIMESTAMP); index += 1
          OracleUtils.ExecPS(data, ps, index, "PJFH", java.sql.Types.NUMERIC); index += 1
          OracleUtils.ExecPS(data, ps, index, "FHL", java.sql.Types.NUMERIC); index += 1
          OracleUtils.ExecPS(data, ps, index, "FZL", java.sql.Types.NUMERIC); index += 1
          OracleUtils.ExecPS(data, ps, index, "AXDYQXYCDS", java.sql.Types.NUMERIC); index += 1
          OracleUtils.ExecPS(data, ps, index, "AXDYQXSCDS", java.sql.Types.NUMERIC); index += 1
          OracleUtils.ExecPS(data, ps, index, "BXDYQXYCDS", java.sql.Types.NUMERIC); index += 1
          OracleUtils.ExecPS(data, ps, index, "BXDYQXSCDS", java.sql.Types.NUMERIC); index += 1
          OracleUtils.ExecPS(data, ps, index, "CXDYQXYCDS", java.sql.Types.NUMERIC); index += 1
          OracleUtils.ExecPS(data, ps, index, "CXDYQXSCDS", java.sql.Types.NUMERIC); index += 1
          OracleUtils.ExecPS(data, ps, index, "AXDLQXYCDS", java.sql.Types.NUMERIC); index += 1
          OracleUtils.ExecPS(data, ps, index, "AXDLQXSCDS", java.sql.Types.NUMERIC); index += 1
          OracleUtils.ExecPS(data, ps, index, "BXDLQXYCDS", java.sql.Types.NUMERIC); index += 1
          OracleUtils.ExecPS(data, ps, index, "BXDLQXSCDS", java.sql.Types.NUMERIC); index += 1
          OracleUtils.ExecPS(data, ps, index, "CXDLQXYCDS", java.sql.Types.NUMERIC); index += 1
          OracleUtils.ExecPS(data, ps, index, "CXDLQXSCDS", java.sql.Types.NUMERIC); index += 1
          OracleUtils.ExecPS(data, ps, index, "GLYSQXYCDS", java.sql.Types.NUMERIC); index += 1
          OracleUtils.ExecPS(data, ps, index, "GLYSQXSCDS", java.sql.Types.NUMERIC); index += 1
          OracleUtils.ExecPS(data, ps, index, "YGZGLQXYCDS", java.sql.Types.NUMERIC); index += 1
          OracleUtils.ExecPS(data, ps, index, "YGZGLQXSCDS", java.sql.Types.NUMERIC); index += 1
          OracleUtils.ExecPS(data, ps, index, "WGZGLQXYCDS", java.sql.Types.NUMERIC); index += 1
          OracleUtils.ExecPS(data, ps, index, "WGZGLQXSCDS", java.sql.Types.NUMERIC);

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

  def insertPBExcept(df: DataFrame, statDate: String, statCycle: String): Unit = {
    val tableName = DateFormatUtil.tableName("PWYW_PBYCTJ", statCycle)
    val errTableName = DateFormatUtil.tableName("ERR_PWYW_PBYCTJ", statCycle)
    OracleUtils.detCurDayData(tableName, statCycle, statDate)

    if (DateFormatUtil.STAT_CYCLE_DAY.equals(statCycle)) {
      val sql = s"""INSERT INTO ${tableName}
        (PBMC,PBID,ZXMC,SSXL,XLMC,SSGT,GTMC,DWBM,DWMC,DWJB,SJDWBM,SJDWMC,BZID,DYDJ,SBZT,TYRQ,CNW,SFDW,ZYCD,PBLX,TJRQ,A00110,A00110_SJ,A00110_CS,A00111,A00111_SJ,A00111_CS,A00115,A00115_SJ,A00115_CS,A00116,A00116_SJ,A00116_CS,A00118,A00118_SJ,A00118_CS,A00112,A00112_SJ,A00112_CS,A00130,A00130_SJ,A00130_CS,A00131,A00131_SJ,A00131_CS,A00132,A00132_SJ,A00132_CS,A00133,A00133_SJ,A00133_CS,A00134,A00134_SJ,A00134_CS,A00135,A00135_SJ,A00135_CS,A00136,A00136_SJ,A00136_CS,A00137,A00137_SJ,A00137_CS,A00138,A00138_SJ,A00138_CS,A00139,A00139_SJ,A00139_CS,A0013A,A0013A_SJ,A0013A_CS,A0013R,A0013R_SJ,A0013R_CS,A0013S,A0013S_SJ,A0013S_CS,A0013T,A0013T_SJ,A0013T_CS,WGQB,WGGB,DYYCGDDS,DYYCGGDS)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)  log errors into ${errTableName}('TEST') reject limit unlimited """

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
            var index: Int = 1
            OracleUtils.ExecPS(data, ps, index, "PBMC", java.sql.Types.VARCHAR); index += 1
            OracleUtils.ExecPS(data, ps, index, "PBID", java.sql.Types.VARCHAR); index += 1
            OracleUtils.ExecPS(data, ps, index, "ZXMC", java.sql.Types.VARCHAR); index += 1
            OracleUtils.ExecPS(data, ps, index, "SSXL", java.sql.Types.VARCHAR); index += 1
            OracleUtils.ExecPS(data, ps, index, "XLMC", java.sql.Types.VARCHAR); index += 1
            OracleUtils.ExecPS(data, ps, index, "SSGT", java.sql.Types.VARCHAR); index += 1
            OracleUtils.ExecPS(data, ps, index, "GTMC", java.sql.Types.VARCHAR); index += 1
            OracleUtils.ExecPS(data, ps, index, "DWBM", java.sql.Types.VARCHAR); index += 1
            OracleUtils.ExecPS(data, ps, index, "DWMC", java.sql.Types.VARCHAR); index += 1
            OracleUtils.ExecPS(data, ps, index, "DWJB", java.sql.Types.VARCHAR); index += 1
            OracleUtils.ExecPS(data, ps, index, "SJDWBM", java.sql.Types.VARCHAR); index += 1
            OracleUtils.ExecPS(data, ps, index, "SJDWMC", java.sql.Types.VARCHAR); index += 1
            OracleUtils.ExecPS(data, ps, index, "BZID", java.sql.Types.VARCHAR); index += 1
            OracleUtils.ExecPS(data, ps, index, "DYDJ", java.sql.Types.VARCHAR); index += 1
            OracleUtils.ExecPS(data, ps, index, "SBZT", java.sql.Types.VARCHAR); index += 1
            OracleUtils.ExecPS(data, ps, index, "TYRQ", java.sql.Types.TIMESTAMP); index += 1
            OracleUtils.ExecPS(data, ps, index, "CNW", java.sql.Types.VARCHAR); index += 1
            OracleUtils.ExecPS(data, ps, index, "SFDW", java.sql.Types.VARCHAR); index += 1
            OracleUtils.ExecPS(data, ps, index, "ZYCD", java.sql.Types.VARCHAR); index += 1
            OracleUtils.ExecPS(data, ps, index, "PBLX", java.sql.Types.VARCHAR); index += 1
            OracleUtils.ExecPS(data, ps, index, "TJRQ", java.sql.Types.DATE); index += 1
            OracleUtils.ExecPS(data, ps, index, "A00110", java.sql.Types.VARCHAR); index += 1
            OracleUtils.ExecPS(data, ps, index, "A00110_SJ", java.sql.Types.NUMERIC); index += 1
            OracleUtils.ExecPS(data, ps, index, "A00110_CS", java.sql.Types.NUMERIC); index += 1
            OracleUtils.ExecPS(data, ps, index, "A00111", java.sql.Types.VARCHAR); index += 1
            OracleUtils.ExecPS(data, ps, index, "A00111_SJ", java.sql.Types.NUMERIC); index += 1
            OracleUtils.ExecPS(data, ps, index, "A00111_CS", java.sql.Types.NUMERIC); index += 1
            OracleUtils.ExecPS(data, ps, index, "A00115", java.sql.Types.VARCHAR); index += 1
            OracleUtils.ExecPS(data, ps, index, "A00115_SJ", java.sql.Types.NUMERIC); index += 1
            OracleUtils.ExecPS(data, ps, index, "A00115_CS", java.sql.Types.NUMERIC); index += 1
            OracleUtils.ExecPS(data, ps, index, "A00116", java.sql.Types.VARCHAR); index += 1
            OracleUtils.ExecPS(data, ps, index, "A00116_SJ", java.sql.Types.NUMERIC); index += 1
            OracleUtils.ExecPS(data, ps, index, "A00116_CS", java.sql.Types.NUMERIC); index += 1
            OracleUtils.ExecPS(data, ps, index, "A00118", java.sql.Types.VARCHAR); index += 1
            OracleUtils.ExecPS(data, ps, index, "A00118_SJ", java.sql.Types.NUMERIC); index += 1
            OracleUtils.ExecPS(data, ps, index, "A00118_CS", java.sql.Types.NUMERIC); index += 1
            OracleUtils.ExecPS(data, ps, index, "A00112", java.sql.Types.VARCHAR); index += 1
            OracleUtils.ExecPS(data, ps, index, "A00112_SJ", java.sql.Types.NUMERIC); index += 1
            OracleUtils.ExecPS(data, ps, index, "A00112_CS", java.sql.Types.NUMERIC); index += 1
            OracleUtils.ExecPS(data, ps, index, "A00130", java.sql.Types.VARCHAR); index += 1
            OracleUtils.ExecPS(data, ps, index, "A00130_SJ", java.sql.Types.NUMERIC); index += 1
            OracleUtils.ExecPS(data, ps, index, "A00130_CS", java.sql.Types.NUMERIC); index += 1
            OracleUtils.ExecPS(data, ps, index, "A00131", java.sql.Types.VARCHAR); index += 1
            OracleUtils.ExecPS(data, ps, index, "A00131_SJ", java.sql.Types.NUMERIC); index += 1
            OracleUtils.ExecPS(data, ps, index, "A00131_CS", java.sql.Types.NUMERIC); index += 1
            OracleUtils.ExecPS(data, ps, index, "A00132", java.sql.Types.VARCHAR); index += 1
            OracleUtils.ExecPS(data, ps, index, "A00132_SJ", java.sql.Types.NUMERIC); index += 1
            OracleUtils.ExecPS(data, ps, index, "A00132_CS", java.sql.Types.NUMERIC); index += 1
            OracleUtils.ExecPS(data, ps, index, "A00133", java.sql.Types.VARCHAR); index += 1
            OracleUtils.ExecPS(data, ps, index, "A00133_SJ", java.sql.Types.NUMERIC); index += 1
            OracleUtils.ExecPS(data, ps, index, "A00133_CS", java.sql.Types.NUMERIC); index += 1
            OracleUtils.ExecPS(data, ps, index, "A00134", java.sql.Types.VARCHAR); index += 1
            OracleUtils.ExecPS(data, ps, index, "A00134_SJ", java.sql.Types.NUMERIC); index += 1
            OracleUtils.ExecPS(data, ps, index, "A00134_CS", java.sql.Types.NUMERIC); index += 1
            OracleUtils.ExecPS(data, ps, index, "A00135", java.sql.Types.VARCHAR); index += 1
            OracleUtils.ExecPS(data, ps, index, "A00135_SJ", java.sql.Types.NUMERIC); index += 1
            OracleUtils.ExecPS(data, ps, index, "A00135_CS", java.sql.Types.NUMERIC); index += 1
            OracleUtils.ExecPS(data, ps, index, "A00136", java.sql.Types.VARCHAR); index += 1
            OracleUtils.ExecPS(data, ps, index, "A00136_SJ", java.sql.Types.NUMERIC); index += 1
            OracleUtils.ExecPS(data, ps, index, "A00136_CS", java.sql.Types.NUMERIC); index += 1
            OracleUtils.ExecPS(data, ps, index, "A00137", java.sql.Types.VARCHAR); index += 1
            OracleUtils.ExecPS(data, ps, index, "A00137_SJ", java.sql.Types.NUMERIC); index += 1
            OracleUtils.ExecPS(data, ps, index, "A00137_CS", java.sql.Types.NUMERIC); index += 1
            OracleUtils.ExecPS(data, ps, index, "A00138", java.sql.Types.VARCHAR); index += 1
            OracleUtils.ExecPS(data, ps, index, "A00138_SJ", java.sql.Types.NUMERIC); index += 1
            OracleUtils.ExecPS(data, ps, index, "A00138_CS", java.sql.Types.NUMERIC); index += 1
            OracleUtils.ExecPS(data, ps, index, "A00139", java.sql.Types.VARCHAR); index += 1
            OracleUtils.ExecPS(data, ps, index, "A00139_SJ", java.sql.Types.NUMERIC); index += 1
            OracleUtils.ExecPS(data, ps, index, "A00139_CS", java.sql.Types.NUMERIC); index += 1
            OracleUtils.ExecPS(data, ps, index, "A0013A", java.sql.Types.VARCHAR); index += 1
            OracleUtils.ExecPS(data, ps, index, "A0013A_SJ", java.sql.Types.NUMERIC); index += 1
            OracleUtils.ExecPS(data, ps, index, "A0013A_CS", java.sql.Types.NUMERIC); index += 1
            OracleUtils.ExecPS(data, ps, index, "A0013R", java.sql.Types.VARCHAR); index += 1
            OracleUtils.ExecPS(data, ps, index, "A0013R_SJ", java.sql.Types.NUMERIC); index += 1
            OracleUtils.ExecPS(data, ps, index, "A0013R_CS", java.sql.Types.NUMERIC); index += 1
            OracleUtils.ExecPS(data, ps, index, "A0013S", java.sql.Types.VARCHAR); index += 1
            OracleUtils.ExecPS(data, ps, index, "A0013S_SJ", java.sql.Types.NUMERIC); index += 1
            OracleUtils.ExecPS(data, ps, index, "A0013S_CS", java.sql.Types.NUMERIC); index += 1
            OracleUtils.ExecPS(data, ps, index, "A0013T", java.sql.Types.VARCHAR); index += 1
            OracleUtils.ExecPS(data, ps, index, "A0013T_SJ", java.sql.Types.NUMERIC); index += 1
            OracleUtils.ExecPS(data, ps, index, "A0013T_CS", java.sql.Types.NUMERIC); index += 1
            OracleUtils.ExecPS(data, ps, index, "WGQBC", java.sql.Types.NUMERIC); index += 1
            OracleUtils.ExecPS(data, ps, index, "WGGBC", java.sql.Types.NUMERIC); index += 1
            OracleUtils.ExecPS(data, ps, index, "DYYCGDDS", java.sql.Types.NUMERIC); index += 1
            OracleUtils.ExecPS(data, ps, index, "DYYCGGDS", java.sql.Types.NUMERIC)

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
//        } catch {
//          case e: Exception => e.printStackTrace()
        } finally {
          if (ps != null) {
            ps.close()
          }
          if (conn != null) {
            conn.close()
          }
        }
      })
    } else {
      val tjrq = if (DateFormatUtil.STAT_CYCLE_WEEK.equals(statCycle)) "TJKSRQ,TJJSRQ" else "TJRQ"
      val tjrqParam = if (DateFormatUtil.STAT_CYCLE_WEEK.equals(statCycle)) "?,?" else "?"
      val sql = s"""INSERT INTO ${tableName}
        (PBID,PBMC,ZXMC,SSXL,XLMC,SSGT,GTMC,DWBM,DWMC,DWJB,CNW,SJDWBM,SJDWMC,BZID,DYDJ,SBZT,TYRQ,SFDW,ZYCD,PBLX,${tjrq},A00110,A00110_SJ,A00110_CS,A00110_TS,A00111,A00111_SJ,A00111_CS,A00111_TS,A00115,A00115_SJ,A00115_CS,A00115_TS,A00116,A00116_SJ,A00116_CS,A00116_TS,A00118,A00118_SJ,A00118_CS,A00118_TS,A00112,A00112_SJ,A00112_CS,A00112_TS,A00130,A00130_SJ,A00130_CS,A00130_TS,A00131,A00131_SJ,A00131_CS,A00131_TS,A00132,A00132_SJ,A00132_CS,A00132_TS,A00133,A00133_SJ,A00133_CS,A00133_TS,A00134,A00134_SJ,A00134_CS,A00134_TS,A00135,A00135_SJ,A00135_CS,A00135_TS,A00136,A00136_SJ,A00136_CS,A00136_TS,A00137,A00137_SJ,A00137_CS,A00137_TS,A00138,A00138_SJ,A00138_CS,A00138_TS,A00139,A00139_SJ,A00139_CS,A00139_TS,A0013A,A0013A_SJ,A0013A_CS,A0013A_TS,A0013R,A0013R_SJ,A0013R_CS,A0013R_TS,A0013S,A0013S_SJ,A0013S_CS,A0013S_TS,A0013T,A0013T_SJ,A0013T_CS,A0013T_TS,REA_LOW_CS,REA_OVER_CS,DYYCGDDS,DYYCGGDS)
        VALUES  (${tjrqParam},?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"""
      
      println(sql)  
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
            var index: Int = 1
            OracleUtils.ExecPS(data, ps, index, "PBID", java.sql.Types.VARCHAR); index += 1
            OracleUtils.ExecPS(data, ps, index, "PBMC", java.sql.Types.VARCHAR); index += 1
            OracleUtils.ExecPS(data, ps, index, "ZXMC", java.sql.Types.VARCHAR); index += 1
            OracleUtils.ExecPS(data, ps, index, "SSXL", java.sql.Types.VARCHAR); index += 1
            OracleUtils.ExecPS(data, ps, index, "XLMC", java.sql.Types.VARCHAR); index += 1
            OracleUtils.ExecPS(data, ps, index, "SSGT", java.sql.Types.VARCHAR); index += 1
            OracleUtils.ExecPS(data, ps, index, "GTMC", java.sql.Types.VARCHAR); index += 1
            OracleUtils.ExecPS(data, ps, index, "DWBM", java.sql.Types.VARCHAR); index += 1
            OracleUtils.ExecPS(data, ps, index, "DWMC", java.sql.Types.VARCHAR); index += 1
            OracleUtils.ExecPS(data, ps, index, "DWJB", java.sql.Types.VARCHAR); index += 1
            OracleUtils.ExecPS(data, ps, index, "CNW", java.sql.Types.VARCHAR); index += 1
            OracleUtils.ExecPS(data, ps, index, "SJDWBM", java.sql.Types.VARCHAR); index += 1
            OracleUtils.ExecPS(data, ps, index, "SJDWMC", java.sql.Types.VARCHAR); index += 1
            OracleUtils.ExecPS(data, ps, index, "BZID", java.sql.Types.VARCHAR); index += 1
            OracleUtils.ExecPS(data, ps, index, "DYDJ", java.sql.Types.VARCHAR); index += 1
            OracleUtils.ExecPS(data, ps, index, "SBZT", java.sql.Types.VARCHAR); index += 1
            OracleUtils.ExecPS(data, ps, index, "TYRQ", java.sql.Types.TIMESTAMP); index += 1
            OracleUtils.ExecPS(data, ps, index, "SFDW", java.sql.Types.VARCHAR); index += 1
            OracleUtils.ExecPS(data, ps, index, "ZYCD", java.sql.Types.VARCHAR); index += 1
            OracleUtils.ExecPS(data, ps, index, "PBLX", java.sql.Types.VARCHAR); index += 1
            if (DateFormatUtil.STAT_CYCLE_WEEK.equals(statCycle)) {
              OracleUtils.ExecPS(data, ps, index, "TJKSRQ", java.sql.Types.DATE); index += 1
              OracleUtils.ExecPS(data, ps, index, "TJJSRQ", java.sql.Types.DATE); index += 1
            } else {
              OracleUtils.ExecPS(data, ps, index, "TJRQ", java.sql.Types.VARCHAR); index += 1
            }
            OracleUtils.ExecPS(data, ps, index, "A00110", java.sql.Types.VARCHAR); index += 1
            OracleUtils.ExecPS(data, ps, index, "A00110_SJ", java.sql.Types.NUMERIC); index += 1
            OracleUtils.ExecPS(data, ps, index, "A00110_CS", java.sql.Types.NUMERIC); index += 1
            OracleUtils.ExecPS(data, ps, index, "A00110_TS", java.sql.Types.NUMERIC); index += 1
            OracleUtils.ExecPS(data, ps, index, "A00111", java.sql.Types.VARCHAR); index += 1
            OracleUtils.ExecPS(data, ps, index, "A00111_SJ", java.sql.Types.NUMERIC); index += 1
            OracleUtils.ExecPS(data, ps, index, "A00111_CS", java.sql.Types.NUMERIC); index += 1
            OracleUtils.ExecPS(data, ps, index, "A00111_TS", java.sql.Types.NUMERIC); index += 1
            OracleUtils.ExecPS(data, ps, index, "A00115", java.sql.Types.VARCHAR); index += 1
            OracleUtils.ExecPS(data, ps, index, "A00115_SJ", java.sql.Types.NUMERIC); index += 1
            OracleUtils.ExecPS(data, ps, index, "A00115_CS", java.sql.Types.NUMERIC); index += 1
            OracleUtils.ExecPS(data, ps, index, "A00115_TS", java.sql.Types.NUMERIC); index += 1
            OracleUtils.ExecPS(data, ps, index, "A00116", java.sql.Types.VARCHAR); index += 1
            OracleUtils.ExecPS(data, ps, index, "A00116_SJ", java.sql.Types.NUMERIC); index += 1
            OracleUtils.ExecPS(data, ps, index, "A00116_CS", java.sql.Types.NUMERIC); index += 1
            OracleUtils.ExecPS(data, ps, index, "A00116_TS", java.sql.Types.NUMERIC); index += 1
            OracleUtils.ExecPS(data, ps, index, "A00118", java.sql.Types.VARCHAR); index += 1
            OracleUtils.ExecPS(data, ps, index, "A00118_SJ", java.sql.Types.NUMERIC); index += 1
            OracleUtils.ExecPS(data, ps, index, "A00118_CS", java.sql.Types.NUMERIC); index += 1
            OracleUtils.ExecPS(data, ps, index, "A00118_TS", java.sql.Types.NUMERIC); index += 1
            OracleUtils.ExecPS(data, ps, index, "A00112", java.sql.Types.VARCHAR); index += 1
            OracleUtils.ExecPS(data, ps, index, "A00112_SJ", java.sql.Types.NUMERIC); index += 1
            OracleUtils.ExecPS(data, ps, index, "A00112_CS", java.sql.Types.NUMERIC); index += 1
            OracleUtils.ExecPS(data, ps, index, "A00112_TS", java.sql.Types.NUMERIC); index += 1
            OracleUtils.ExecPS(data, ps, index, "A00130", java.sql.Types.VARCHAR); index += 1
            OracleUtils.ExecPS(data, ps, index, "A00130_SJ", java.sql.Types.NUMERIC); index += 1
            OracleUtils.ExecPS(data, ps, index, "A00130_CS", java.sql.Types.NUMERIC); index += 1
            OracleUtils.ExecPS(data, ps, index, "A00130_TS", java.sql.Types.NUMERIC); index += 1
            OracleUtils.ExecPS(data, ps, index, "A00131", java.sql.Types.VARCHAR); index += 1
            OracleUtils.ExecPS(data, ps, index, "A00131_SJ", java.sql.Types.NUMERIC); index += 1
            OracleUtils.ExecPS(data, ps, index, "A00131_CS", java.sql.Types.NUMERIC); index += 1
            OracleUtils.ExecPS(data, ps, index, "A00131_TS", java.sql.Types.NUMERIC); index += 1
            OracleUtils.ExecPS(data, ps, index, "A00132", java.sql.Types.VARCHAR); index += 1
            OracleUtils.ExecPS(data, ps, index, "A00132_SJ", java.sql.Types.NUMERIC); index += 1
            OracleUtils.ExecPS(data, ps, index, "A00132_CS", java.sql.Types.NUMERIC); index += 1
            OracleUtils.ExecPS(data, ps, index, "A00132_TS", java.sql.Types.NUMERIC); index += 1
            OracleUtils.ExecPS(data, ps, index, "A00133", java.sql.Types.VARCHAR); index += 1
            OracleUtils.ExecPS(data, ps, index, "A00133_SJ", java.sql.Types.NUMERIC); index += 1
            OracleUtils.ExecPS(data, ps, index, "A00133_CS", java.sql.Types.NUMERIC); index += 1
            OracleUtils.ExecPS(data, ps, index, "A00133_TS", java.sql.Types.NUMERIC); index += 1
            OracleUtils.ExecPS(data, ps, index, "A00134", java.sql.Types.VARCHAR); index += 1
            OracleUtils.ExecPS(data, ps, index, "A00134_SJ", java.sql.Types.NUMERIC); index += 1
            OracleUtils.ExecPS(data, ps, index, "A00134_CS", java.sql.Types.NUMERIC); index += 1
            OracleUtils.ExecPS(data, ps, index, "A00134_TS", java.sql.Types.NUMERIC); index += 1
            OracleUtils.ExecPS(data, ps, index, "A00135", java.sql.Types.VARCHAR); index += 1
            OracleUtils.ExecPS(data, ps, index, "A00135_SJ", java.sql.Types.NUMERIC); index += 1
            OracleUtils.ExecPS(data, ps, index, "A00135_CS", java.sql.Types.NUMERIC); index += 1
            OracleUtils.ExecPS(data, ps, index, "A00135_TS", java.sql.Types.NUMERIC); index += 1
            OracleUtils.ExecPS(data, ps, index, "A00136", java.sql.Types.VARCHAR); index += 1
            OracleUtils.ExecPS(data, ps, index, "A00136_SJ", java.sql.Types.NUMERIC); index += 1
            OracleUtils.ExecPS(data, ps, index, "A00136_CS", java.sql.Types.NUMERIC); index += 1
            OracleUtils.ExecPS(data, ps, index, "A00136_TS", java.sql.Types.NUMERIC); index += 1
            OracleUtils.ExecPS(data, ps, index, "A00137", java.sql.Types.VARCHAR); index += 1
            OracleUtils.ExecPS(data, ps, index, "A00137_SJ", java.sql.Types.NUMERIC); index += 1
            OracleUtils.ExecPS(data, ps, index, "A00137_CS", java.sql.Types.NUMERIC); index += 1
            OracleUtils.ExecPS(data, ps, index, "A00137_TS", java.sql.Types.NUMERIC); index += 1
            OracleUtils.ExecPS(data, ps, index, "A00138", java.sql.Types.VARCHAR); index += 1
            OracleUtils.ExecPS(data, ps, index, "A00138_SJ", java.sql.Types.NUMERIC); index += 1
            OracleUtils.ExecPS(data, ps, index, "A00138_CS", java.sql.Types.NUMERIC); index += 1
            OracleUtils.ExecPS(data, ps, index, "A00138_TS", java.sql.Types.NUMERIC); index += 1
            OracleUtils.ExecPS(data, ps, index, "A00139", java.sql.Types.VARCHAR); index += 1
            OracleUtils.ExecPS(data, ps, index, "A00139_SJ", java.sql.Types.NUMERIC); index += 1
            OracleUtils.ExecPS(data, ps, index, "A00139_CS", java.sql.Types.NUMERIC); index += 1
            OracleUtils.ExecPS(data, ps, index, "A00139_TS", java.sql.Types.NUMERIC); index += 1
            OracleUtils.ExecPS(data, ps, index, "A0013A", java.sql.Types.VARCHAR); index += 1
            OracleUtils.ExecPS(data, ps, index, "A0013A_SJ", java.sql.Types.NUMERIC); index += 1
            OracleUtils.ExecPS(data, ps, index, "A0013A_CS", java.sql.Types.NUMERIC); index += 1
            OracleUtils.ExecPS(data, ps, index, "A0013A_TS", java.sql.Types.NUMERIC); index += 1
            OracleUtils.ExecPS(data, ps, index, "A0013R", java.sql.Types.VARCHAR); index += 1
            OracleUtils.ExecPS(data, ps, index, "A0013R_SJ", java.sql.Types.NUMERIC); index += 1
            OracleUtils.ExecPS(data, ps, index, "A0013R_CS", java.sql.Types.NUMERIC); index += 1
            OracleUtils.ExecPS(data, ps, index, "A0013R_TS", java.sql.Types.NUMERIC); index += 1
            OracleUtils.ExecPS(data, ps, index, "A0013S", java.sql.Types.VARCHAR); index += 1
            OracleUtils.ExecPS(data, ps, index, "A0013S_SJ", java.sql.Types.NUMERIC); index += 1
            OracleUtils.ExecPS(data, ps, index, "A0013S_CS", java.sql.Types.NUMERIC); index += 1
            OracleUtils.ExecPS(data, ps, index, "A0013S_TS", java.sql.Types.NUMERIC); index += 1
            OracleUtils.ExecPS(data, ps, index, "A0013T", java.sql.Types.VARCHAR); index += 1
            OracleUtils.ExecPS(data, ps, index, "A0013T_SJ", java.sql.Types.NUMERIC); index += 1
            OracleUtils.ExecPS(data, ps, index, "A0013T_CS", java.sql.Types.NUMERIC); index += 1
            OracleUtils.ExecPS(data, ps, index, "A0013T_TS", java.sql.Types.NUMERIC); index += 1
            OracleUtils.ExecPS(data, ps, index, "WGQBC", java.sql.Types.NUMERIC); index += 1
            OracleUtils.ExecPS(data, ps, index, "WGGBC", java.sql.Types.NUMERIC); index += 1
            OracleUtils.ExecPS(data, ps, index, "DYYCGDDS", java.sql.Types.NUMERIC); index += 1
            OracleUtils.ExecPS(data, ps, index, "DYYCGGDS", java.sql.Types.NUMERIC); index += 1

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
//        } catch {
//          case e: Exception => e.printStackTrace()
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

  def insertSSPBExcept(df: DataFrame, statDate: String, statCycle: String): Unit = {
    val tableName = DateFormatUtil.tableName("PWYW_DWPBTJ_SS", statCycle)
    val errTableName = DateFormatUtil.tableName("ERR_PWYW_DWPBTJ_SS", statCycle)

    OracleUtils.detCurDayData(tableName, statCycle, statDate)

    val tjrq = if (DateFormatUtil.STAT_CYCLE_WEEK.equals(statCycle)) "TJKSRQ,TJJSRQ" else "TJRQ"
    val tjrqParam = if (DateFormatUtil.STAT_CYCLE_WEEK.equals(statCycle)) "?,?" else "?"
    val sql =
      s"INSERT INTO ${tableName}\n" +
        s"(DWBM,DWMC,DWJB,SJ_DWBM,SJ_DWMC,PBLX,CNW,${tjrq},PMSPBSL,YCFGPBSL,CJPBSL,GDYTS,GDYZB,GDYCS,GDYSC,DDYTS,DDYZB,DDYCS,DDYSC,DLSXBPHTS,DLSXBPHZB,DLSXBPHCS,DLSXBPHSC,GZTS,GZZB,GZCS,GZSC,GLYSYCTS,GLYSYCZB,GLYSYCCS,GLYSYCSC,DYSXBPHTS,DYSXBPHZB,DYSXBPHCS,DYSXBPHSC,AGZTS,AGZZB,AGZCS,AGZSC,BGZTS,BGZZB,BGZCS,BGZSC,CGZTS,CGZZB,CGZCS,CGZSC,AZZTS,AZZZB,AZZCS,AZZSC,BZZTS,BZZZB,BZZCS,BZZSC,CZZTS,CZZZB,CZZCS,CZZSC,ZZTS,ZZZB,ZZCS,ZZSC,QZTS,QZZB,QZCS,QZSC,DYYCTS,DYYCZB,FZYCTS,FZYCZB)\n" +
        s"VALUES  (?,?,?,?,?,?,?,${tjrqParam},?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)  " //log errors into ${errTableName}('TEST')"

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
          var index: Int = 1
          OracleUtils.ExecPS(data, ps, index, "DWBM", java.sql.Types.VARCHAR); index += 1
          OracleUtils.ExecPS(data, ps, index, "DWMC", java.sql.Types.VARCHAR); index += 1
          OracleUtils.ExecPS(data, ps, index, "DWJB", java.sql.Types.VARCHAR); index += 1
          OracleUtils.ExecPS(data, ps, index, "SJDWBM", java.sql.Types.VARCHAR); index += 1
          OracleUtils.ExecPS(data, ps, index, "SJDWMC", java.sql.Types.VARCHAR); index += 1
          OracleUtils.ExecPS(data, ps, index, "PBLX", java.sql.Types.VARCHAR); index += 1
          OracleUtils.ExecPS(data, ps, index, "CNW", java.sql.Types.VARCHAR); index += 1
          if (DateFormatUtil.STAT_CYCLE_WEEK.equals(statCycle)) {
            OracleUtils.ExecPS(data, ps, index, "TJKSRQ", java.sql.Types.DATE); index += 1
            OracleUtils.ExecPS(data, ps, index, "TJJSRQ", java.sql.Types.DATE); index += 1
          } else if (DateFormatUtil.STAT_CYCLE_DAY.equals(statCycle)) {
            OracleUtils.ExecPS(data, ps, index, "TJRQ", java.sql.Types.DATE); index += 1
          } else {
            OracleUtils.ExecPS(data, ps, index, "TJRQ", java.sql.Types.VARCHAR); index += 1
          }
          OracleUtils.ExecPS(data, ps, index, "PMSPBSL", java.sql.Types.NUMERIC); index += 1
          OracleUtils.ExecPS(data, ps, index, "YCFGPBSL", java.sql.Types.NUMERIC); index += 1
          OracleUtils.ExecPS(data, ps, index, "CJPBSL", java.sql.Types.NUMERIC); index += 1
          OracleUtils.ExecPS(data, ps, index, "GDYTS", java.sql.Types.NUMERIC); index += 1
          OracleUtils.ExecPS(data, ps, index, "GDYZB", java.sql.Types.NUMERIC); index += 1
          OracleUtils.ExecPS(data, ps, index, "GDYCS", java.sql.Types.NUMERIC); index += 1
          OracleUtils.ExecPS(data, ps, index, "GDYSC", java.sql.Types.NUMERIC); index += 1
          OracleUtils.ExecPS(data, ps, index, "DDYTS", java.sql.Types.NUMERIC); index += 1
          OracleUtils.ExecPS(data, ps, index, "DDYZB", java.sql.Types.NUMERIC); index += 1
          OracleUtils.ExecPS(data, ps, index, "DDYCS", java.sql.Types.NUMERIC); index += 1
          OracleUtils.ExecPS(data, ps, index, "DDYSC", java.sql.Types.NUMERIC); index += 1
          OracleUtils.ExecPS(data, ps, index, "DLSXBPHTS", java.sql.Types.NUMERIC); index += 1
          OracleUtils.ExecPS(data, ps, index, "DLSXBPHZB", java.sql.Types.NUMERIC); index += 1
          OracleUtils.ExecPS(data, ps, index, "DLSXBPHCS", java.sql.Types.NUMERIC); index += 1
          OracleUtils.ExecPS(data, ps, index, "DLSXBPHSC", java.sql.Types.NUMERIC); index += 1
          OracleUtils.ExecPS(data, ps, index, "GZTS", java.sql.Types.NUMERIC); index += 1
          OracleUtils.ExecPS(data, ps, index, "GZZB", java.sql.Types.NUMERIC); index += 1
          OracleUtils.ExecPS(data, ps, index, "GZCS", java.sql.Types.NUMERIC); index += 1
          OracleUtils.ExecPS(data, ps, index, "GZSC", java.sql.Types.NUMERIC); index += 1
          OracleUtils.ExecPS(data, ps, index, "GLYSYCTS", java.sql.Types.NUMERIC); index += 1
          OracleUtils.ExecPS(data, ps, index, "GLYSYCZB", java.sql.Types.NUMERIC); index += 1
          OracleUtils.ExecPS(data, ps, index, "GLYSYCCS", java.sql.Types.NUMERIC); index += 1
          OracleUtils.ExecPS(data, ps, index, "GLYSYCSC", java.sql.Types.NUMERIC); index += 1
          OracleUtils.ExecPS(data, ps, index, "DYSXBPHTS", java.sql.Types.NUMERIC); index += 1
          OracleUtils.ExecPS(data, ps, index, "DYSXBPHZB", java.sql.Types.NUMERIC); index += 1
          OracleUtils.ExecPS(data, ps, index, "DYSXBPHCS", java.sql.Types.NUMERIC); index += 1
          OracleUtils.ExecPS(data, ps, index, "DYSXBPHSC", java.sql.Types.NUMERIC); index += 1
          OracleUtils.ExecPS(data, ps, index, "AGZTS", java.sql.Types.NUMERIC); index += 1
          OracleUtils.ExecPS(data, ps, index, "AGZZB", java.sql.Types.NUMERIC); index += 1
          OracleUtils.ExecPS(data, ps, index, "AGZCS", java.sql.Types.NUMERIC); index += 1
          OracleUtils.ExecPS(data, ps, index, "AGZSC", java.sql.Types.NUMERIC); index += 1
          OracleUtils.ExecPS(data, ps, index, "BGZTS", java.sql.Types.NUMERIC); index += 1
          OracleUtils.ExecPS(data, ps, index, "BGZZB", java.sql.Types.NUMERIC); index += 1
          OracleUtils.ExecPS(data, ps, index, "BGZCS", java.sql.Types.NUMERIC); index += 1
          OracleUtils.ExecPS(data, ps, index, "BGZSC", java.sql.Types.NUMERIC); index += 1
          OracleUtils.ExecPS(data, ps, index, "CGZTS", java.sql.Types.NUMERIC); index += 1
          OracleUtils.ExecPS(data, ps, index, "CGZZB", java.sql.Types.NUMERIC); index += 1
          OracleUtils.ExecPS(data, ps, index, "CGZCS", java.sql.Types.NUMERIC); index += 1
          OracleUtils.ExecPS(data, ps, index, "CGZSC", java.sql.Types.NUMERIC); index += 1
          OracleUtils.ExecPS(data, ps, index, "AZZTS", java.sql.Types.NUMERIC); index += 1
          OracleUtils.ExecPS(data, ps, index, "AZZZB", java.sql.Types.NUMERIC); index += 1
          OracleUtils.ExecPS(data, ps, index, "AZZCS", java.sql.Types.NUMERIC); index += 1
          OracleUtils.ExecPS(data, ps, index, "AZZSC", java.sql.Types.NUMERIC); index += 1
          OracleUtils.ExecPS(data, ps, index, "BZZTS", java.sql.Types.NUMERIC); index += 1
          OracleUtils.ExecPS(data, ps, index, "BZZZB", java.sql.Types.NUMERIC); index += 1
          OracleUtils.ExecPS(data, ps, index, "BZZCS", java.sql.Types.NUMERIC); index += 1
          OracleUtils.ExecPS(data, ps, index, "BZZSC", java.sql.Types.NUMERIC); index += 1
          OracleUtils.ExecPS(data, ps, index, "CZZTS", java.sql.Types.NUMERIC); index += 1
          OracleUtils.ExecPS(data, ps, index, "CZZZB", java.sql.Types.NUMERIC); index += 1
          OracleUtils.ExecPS(data, ps, index, "CZZCS", java.sql.Types.NUMERIC); index += 1
          OracleUtils.ExecPS(data, ps, index, "CZZSC", java.sql.Types.NUMERIC); index += 1
          //          OracleUtils.ExecPS(data, ps, index, "AQZTS", java.sql.Types.NUMERIC); index += 1
          //          OracleUtils.ExecPS(data, ps, index, "AQZZB", java.sql.Types.NUMERIC); index += 1
          //          OracleUtils.ExecPS(data, ps, index, "AQZCS", java.sql.Types.NUMERIC); index += 1
          //          OracleUtils.ExecPS(data, ps, index, "AQZSC", java.sql.Types.NUMERIC); index += 1
          //          OracleUtils.ExecPS(data, ps, index, "BQZTS", java.sql.Types.NUMERIC); index += 1
          //          OracleUtils.ExecPS(data, ps, index, "BQZZB", java.sql.Types.NUMERIC); index += 1
          //          OracleUtils.ExecPS(data, ps, index, "BQZCS", java.sql.Types.NUMERIC); index += 1
          //          OracleUtils.ExecPS(data, ps, index, "BQZSC", java.sql.Types.NUMERIC); index += 1
          //          OracleUtils.ExecPS(data, ps, index, "CQZTS", java.sql.Types.NUMERIC); index += 1
          //          OracleUtils.ExecPS(data, ps, index, "CQZZB", java.sql.Types.NUMERIC); index += 1
          //          OracleUtils.ExecPS(data, ps, index, "CQZCS", java.sql.Types.NUMERIC); index += 1
          //          OracleUtils.ExecPS(data, ps, index, "CQZSC", java.sql.Types.NUMERIC); index += 1
          OracleUtils.ExecPS(data, ps, index, "ZZTS", java.sql.Types.NUMERIC); index += 1
          OracleUtils.ExecPS(data, ps, index, "ZZZB", java.sql.Types.NUMERIC); index += 1
          OracleUtils.ExecPS(data, ps, index, "ZZCS", java.sql.Types.NUMERIC); index += 1
          OracleUtils.ExecPS(data, ps, index, "ZZSC", java.sql.Types.NUMERIC); index += 1
          OracleUtils.ExecPS(data, ps, index, "QZTS", java.sql.Types.NUMERIC); index += 1
          OracleUtils.ExecPS(data, ps, index, "QZZB", java.sql.Types.NUMERIC); index += 1
          OracleUtils.ExecPS(data, ps, index, "QZCS", java.sql.Types.NUMERIC); index += 1
          OracleUtils.ExecPS(data, ps, index, "QZSC", java.sql.Types.NUMERIC); index += 1
          OracleUtils.ExecPS(data, ps, index, "DYYCTS", java.sql.Types.NUMERIC); index += 1
          OracleUtils.ExecPS(data, ps, index, "DYYCZB", java.sql.Types.NUMERIC); index += 1
          OracleUtils.ExecPS(data, ps, index, "FZYCTS", java.sql.Types.NUMERIC); index += 1
          OracleUtils.ExecPS(data, ps, index, "FZYCZB", java.sql.Types.NUMERIC); index += 1
//          OracleUtils.ExecPS(data, ps, index, "FZL1TS", java.sql.Types.NUMERIC); index += 1
//          OracleUtils.ExecPS(data, ps, index, "FZL2TS", java.sql.Types.NUMERIC); index += 1
//          OracleUtils.ExecPS(data, ps, index, "FZL3TS", java.sql.Types.NUMERIC); index += 1
//          OracleUtils.ExecPS(data, ps, index, "FZL4TS", java.sql.Types.NUMERIC); index += 1

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

  def insertBZPBExcept(df: DataFrame, statDate: String, statCycle: String): Unit = {
    val tableName = DateFormatUtil.tableName("PWYW_DWPBTJ_BZ", statCycle)
    val errTableName = DateFormatUtil.tableName("ERR_PWYW_DWPBTJ_BZ", statCycle)

    OracleUtils.detCurDayData(tableName, statCycle, statDate)

    val tjrq = if (DateFormatUtil.STAT_CYCLE_WEEK.equals(statCycle)) "TJKSRQ,TJJSRQ" else "TJRQ"
    val tjrqParam = if (DateFormatUtil.STAT_CYCLE_WEEK.equals(statCycle)) "?,?" else "?"
    val sql =
      s"INSERT INTO ${tableName}\n" +
        s"(DWBM,DWMC,DWJB,SJ_DWBM,SJ_DWMC,PBLX,CNW,${tjrq},PMSPBSL,YCFGPBSL,CJPBSL,GDYTS,GDYZB,GDYCS,GDYSC,DDYTS,DDYZB,DDYCS,DDYSC,DLSXBPHTS,DLSXBPHZB,DLSXBPHCS,DLSXBPHSC,GZTS,GZZB,GZCS,GZSC,GLYSYCTS,GLYSYCZB,GLYSYCCS,GLYSYCSC,DYSXBPHTS,DYSXBPHZB,DYSXBPHCS,DYSXBPHSC,AGZTS,AGZZB,AGZCS,AGZSC,BGZTS,BGZZB,BGZCS,BGZSC,CGZTS,CGZZB,CGZCS,CGZSC,AZZTS,AZZZB,AZZCS,AZZSC,BZZTS,BZZZB,BZZCS,BZZSC,CZZTS,CZZZB,CZZCS,CZZSC,ZZTS,ZZZB,ZZCS,ZZSC,QZTS,QZZB,QZCS,QZSC,DYYCTS,DYYCZB,FZYCTS,FZYCZB)\n" +
        s"VALUES  (?,?,?,?,?,?,?,${tjrqParam},?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)  " //log errors into ${errTableName}('TEST')"

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
          var index: Int = 1
          OracleUtils.ExecPS(data, ps, index, "DWBM", java.sql.Types.VARCHAR); index += 1
          OracleUtils.ExecPS(data, ps, index, "DWMC", java.sql.Types.VARCHAR); index += 1
          OracleUtils.ExecPS(data, ps, index, "DWJB", java.sql.Types.VARCHAR); index += 1
          OracleUtils.ExecPS(data, ps, index, "SJDWBM", java.sql.Types.VARCHAR); index += 1
          OracleUtils.ExecPS(data, ps, index, "SJDWMC", java.sql.Types.VARCHAR); index += 1
          OracleUtils.ExecPS(data, ps, index, "PBLX", java.sql.Types.VARCHAR); index += 1
          OracleUtils.ExecPS(data, ps, index, "CNW", java.sql.Types.VARCHAR); index += 1
          if (DateFormatUtil.STAT_CYCLE_WEEK.equals(statCycle)) {
            OracleUtils.ExecPS(data, ps, index, "TJKSRQ", java.sql.Types.DATE); index += 1
            OracleUtils.ExecPS(data, ps, index, "TJJSRQ", java.sql.Types.DATE); index += 1
          } else if (DateFormatUtil.STAT_CYCLE_DAY.equals(statCycle)) {
            OracleUtils.ExecPS(data, ps, index, "TJRQ", java.sql.Types.DATE); index += 1
          } else {
            OracleUtils.ExecPS(data, ps, index, "TJRQ", java.sql.Types.VARCHAR); index += 1
          }
          OracleUtils.ExecPS(data, ps, index, "PMSPBSL", java.sql.Types.NUMERIC); index += 1
          OracleUtils.ExecPS(data, ps, index, "YCFGPBSL", java.sql.Types.NUMERIC); index += 1
          OracleUtils.ExecPS(data, ps, index, "CJPBSL", java.sql.Types.NUMERIC); index += 1
          OracleUtils.ExecPS(data, ps, index, "GDYTS", java.sql.Types.NUMERIC); index += 1
          OracleUtils.ExecPS(data, ps, index, "GDYZB", java.sql.Types.NUMERIC); index += 1
          OracleUtils.ExecPS(data, ps, index, "GDYCS", java.sql.Types.NUMERIC); index += 1
          OracleUtils.ExecPS(data, ps, index, "GDYSC", java.sql.Types.NUMERIC); index += 1
          OracleUtils.ExecPS(data, ps, index, "DDYTS", java.sql.Types.NUMERIC); index += 1
          OracleUtils.ExecPS(data, ps, index, "DDYZB", java.sql.Types.NUMERIC); index += 1
          OracleUtils.ExecPS(data, ps, index, "DDYCS", java.sql.Types.NUMERIC); index += 1
          OracleUtils.ExecPS(data, ps, index, "DDYSC", java.sql.Types.NUMERIC); index += 1
          OracleUtils.ExecPS(data, ps, index, "DLSXBPHTS", java.sql.Types.NUMERIC); index += 1
          OracleUtils.ExecPS(data, ps, index, "DLSXBPHZB", java.sql.Types.NUMERIC); index += 1
          OracleUtils.ExecPS(data, ps, index, "DLSXBPHCS", java.sql.Types.NUMERIC); index += 1
          OracleUtils.ExecPS(data, ps, index, "DLSXBPHSC", java.sql.Types.NUMERIC); index += 1
          OracleUtils.ExecPS(data, ps, index, "GZTS", java.sql.Types.NUMERIC); index += 1
          OracleUtils.ExecPS(data, ps, index, "GZZB", java.sql.Types.NUMERIC); index += 1
          OracleUtils.ExecPS(data, ps, index, "GZCS", java.sql.Types.NUMERIC); index += 1
          OracleUtils.ExecPS(data, ps, index, "GZSC", java.sql.Types.NUMERIC); index += 1
          OracleUtils.ExecPS(data, ps, index, "GLYSYCTS", java.sql.Types.NUMERIC); index += 1
          OracleUtils.ExecPS(data, ps, index, "GLYSYCZB", java.sql.Types.NUMERIC); index += 1
          OracleUtils.ExecPS(data, ps, index, "GLYSYCCS", java.sql.Types.NUMERIC); index += 1
          OracleUtils.ExecPS(data, ps, index, "GLYSYCSC", java.sql.Types.NUMERIC); index += 1
          OracleUtils.ExecPS(data, ps, index, "DYSXBPHTS", java.sql.Types.NUMERIC); index += 1
          OracleUtils.ExecPS(data, ps, index, "DYSXBPHZB", java.sql.Types.NUMERIC); index += 1
          OracleUtils.ExecPS(data, ps, index, "DYSXBPHCS", java.sql.Types.NUMERIC); index += 1
          OracleUtils.ExecPS(data, ps, index, "DYSXBPHSC", java.sql.Types.NUMERIC); index += 1
          OracleUtils.ExecPS(data, ps, index, "AGZTS", java.sql.Types.NUMERIC); index += 1
          OracleUtils.ExecPS(data, ps, index, "AGZZB", java.sql.Types.NUMERIC); index += 1
          OracleUtils.ExecPS(data, ps, index, "AGZCS", java.sql.Types.NUMERIC); index += 1
          OracleUtils.ExecPS(data, ps, index, "AGZSC", java.sql.Types.NUMERIC); index += 1
          OracleUtils.ExecPS(data, ps, index, "BGZTS", java.sql.Types.NUMERIC); index += 1
          OracleUtils.ExecPS(data, ps, index, "BGZZB", java.sql.Types.NUMERIC); index += 1
          OracleUtils.ExecPS(data, ps, index, "BGZCS", java.sql.Types.NUMERIC); index += 1
          OracleUtils.ExecPS(data, ps, index, "BGZSC", java.sql.Types.NUMERIC); index += 1
          OracleUtils.ExecPS(data, ps, index, "CGZTS", java.sql.Types.NUMERIC); index += 1
          OracleUtils.ExecPS(data, ps, index, "CGZZB", java.sql.Types.NUMERIC); index += 1
          OracleUtils.ExecPS(data, ps, index, "CGZCS", java.sql.Types.NUMERIC); index += 1
          OracleUtils.ExecPS(data, ps, index, "CGZSC", java.sql.Types.NUMERIC); index += 1
          OracleUtils.ExecPS(data, ps, index, "AZZTS", java.sql.Types.NUMERIC); index += 1
          OracleUtils.ExecPS(data, ps, index, "AZZZB", java.sql.Types.NUMERIC); index += 1
          OracleUtils.ExecPS(data, ps, index, "AZZCS", java.sql.Types.NUMERIC); index += 1
          OracleUtils.ExecPS(data, ps, index, "AZZSC", java.sql.Types.NUMERIC); index += 1
          OracleUtils.ExecPS(data, ps, index, "BZZTS", java.sql.Types.NUMERIC); index += 1
          OracleUtils.ExecPS(data, ps, index, "BZZZB", java.sql.Types.NUMERIC); index += 1
          OracleUtils.ExecPS(data, ps, index, "BZZCS", java.sql.Types.NUMERIC); index += 1
          OracleUtils.ExecPS(data, ps, index, "BZZSC", java.sql.Types.NUMERIC); index += 1
          OracleUtils.ExecPS(data, ps, index, "CZZTS", java.sql.Types.NUMERIC); index += 1
          OracleUtils.ExecPS(data, ps, index, "CZZZB", java.sql.Types.NUMERIC); index += 1
          OracleUtils.ExecPS(data, ps, index, "CZZCS", java.sql.Types.NUMERIC); index += 1
          OracleUtils.ExecPS(data, ps, index, "CZZSC", java.sql.Types.NUMERIC); index += 1
          //          OracleUtils.ExecPS(data, ps, index, "AQZTS", java.sql.Types.NUMERIC); index += 1
          //          OracleUtils.ExecPS(data, ps, index, "AQZZB", java.sql.Types.NUMERIC); index += 1
          //          OracleUtils.ExecPS(data, ps, index, "AQZCS", java.sql.Types.NUMERIC); index += 1
          //          OracleUtils.ExecPS(data, ps, index, "AQZSC", java.sql.Types.NUMERIC); index += 1
          //          OracleUtils.ExecPS(data, ps, index, "BQZTS", java.sql.Types.NUMERIC); index += 1
          //          OracleUtils.ExecPS(data, ps, index, "BQZZB", java.sql.Types.NUMERIC); index += 1
          //          OracleUtils.ExecPS(data, ps, index, "BQZCS", java.sql.Types.NUMERIC); index += 1
          //          OracleUtils.ExecPS(data, ps, index, "BQZSC", java.sql.Types.NUMERIC); index += 1
          //          OracleUtils.ExecPS(data, ps, index, "CQZTS", java.sql.Types.NUMERIC); index += 1
          //          OracleUtils.ExecPS(data, ps, index, "CQZZB", java.sql.Types.NUMERIC); index += 1
          //          OracleUtils.ExecPS(data, ps, index, "CQZCS", java.sql.Types.NUMERIC); index += 1
          //          OracleUtils.ExecPS(data, ps, index, "CQZSC", java.sql.Types.NUMERIC); index += 1
          OracleUtils.ExecPS(data, ps, index, "ZZTS", java.sql.Types.NUMERIC); index += 1
          OracleUtils.ExecPS(data, ps, index, "ZZZB", java.sql.Types.NUMERIC); index += 1
          OracleUtils.ExecPS(data, ps, index, "ZZCS", java.sql.Types.NUMERIC); index += 1
          OracleUtils.ExecPS(data, ps, index, "ZZSC", java.sql.Types.NUMERIC); index += 1
          OracleUtils.ExecPS(data, ps, index, "QZTS", java.sql.Types.NUMERIC); index += 1
          OracleUtils.ExecPS(data, ps, index, "QZZB", java.sql.Types.NUMERIC); index += 1
          OracleUtils.ExecPS(data, ps, index, "QZCS", java.sql.Types.NUMERIC); index += 1
          OracleUtils.ExecPS(data, ps, index, "QZSC", java.sql.Types.NUMERIC); index += 1
          OracleUtils.ExecPS(data, ps, index, "DYYCTS", java.sql.Types.NUMERIC); index += 1
          OracleUtils.ExecPS(data, ps, index, "DYYCZB", java.sql.Types.NUMERIC); index += 1
          OracleUtils.ExecPS(data, ps, index, "FZYCTS", java.sql.Types.NUMERIC); index += 1
          OracleUtils.ExecPS(data, ps, index, "FZYCZB", java.sql.Types.NUMERIC); index += 1
//          OracleUtils.ExecPS(data, ps, index, "FZL1TS", java.sql.Types.NUMERIC); index += 1
//          OracleUtils.ExecPS(data, ps, index, "FZL2TS", java.sql.Types.NUMERIC); index += 1
//          OracleUtils.ExecPS(data, ps, index, "FZL3TS", java.sql.Types.NUMERIC); index += 1
//          OracleUtils.ExecPS(data, ps, index, "FZL4TS", java.sql.Types.NUMERIC); index += 1

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

  def insertPBSSGatherSucc(df: DataFrame, statDate: String, statCycle: String): Unit = {
    val tableName = DateFormatUtil.tableName("PWYW_DWPBQXCJWZLTJ_SS", statCycle)
    val errTableName = DateFormatUtil.tableName("ERR_PWYW_DWPBQXCJWZLTJ_SS", statCycle)

    OracleUtils.detCurDayData(tableName, statCycle, statDate)

    val tjrq = if (DateFormatUtil.STAT_CYCLE_WEEK.equals(statCycle)) "TJKSRQ,TJJSRQ" else "TJRQ"
    val tjrqParam = if (DateFormatUtil.STAT_CYCLE_WEEK.equals(statCycle)) "?,?" else "?"
    val sql =
      s"INSERT INTO ${tableName}\n" +
        s"(DWBM,DWMC,DWJB,SJ_DWBM,SJ_DWMC,PBLX,CNW,PMSPBSL,YCFGPBSL,CJPBSL,${tjrq},AXDYQXYCDS,AXDYQXSCDS,BXDYQXYCDS,BXDYQXSCDS,CXDYQXYCDS,CXDYQXSCDS,AXDLQXYCDS,AXDLQXSCDS,BXDLQXYCDS,BXDLQXSCDS,CXDLQXYCDS,CXDLQXSCDS,YGZGLQXYCDS,YGZGLQXSCDS,WGZGLQXYCDS,WGZGLQXSCDS,GLYSQXYCDS,GLYSQXSCDS)\n" +
        s"VALUES  (?,?,?,?,?,?,?,?,?,?,${tjrqParam},?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)  " //log errors into ${errTableName}('TEST')"

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
          var index: Int = 1

          OracleUtils.ExecPS(data, ps, index, "DWBM", java.sql.Types.VARCHAR); index += 1
          OracleUtils.ExecPS(data, ps, index, "DWMC", java.sql.Types.VARCHAR); index += 1
          OracleUtils.ExecPS(data, ps, index, "DWJB", java.sql.Types.VARCHAR); index += 1
          OracleUtils.ExecPS(data, ps, index, "SJDWBM", java.sql.Types.VARCHAR); index += 1
          OracleUtils.ExecPS(data, ps, index, "SJDWMC", java.sql.Types.VARCHAR); index += 1
          OracleUtils.ExecPS(data, ps, index, "PBLX", java.sql.Types.VARCHAR); index += 1
          OracleUtils.ExecPS(data, ps, index, "CNW", java.sql.Types.VARCHAR); index += 1
          OracleUtils.ExecPS(data, ps, index, "PMSPBSL", java.sql.Types.NUMERIC); index += 1
          OracleUtils.ExecPS(data, ps, index, "YCFGPBSL", java.sql.Types.NUMERIC); index += 1
          OracleUtils.ExecPS(data, ps, index, "CJPBSL", java.sql.Types.NUMERIC); index += 1
          if (DateFormatUtil.STAT_CYCLE_WEEK.equals(statCycle)) {
            OracleUtils.ExecPS(data, ps, index, "TJKSRQ", java.sql.Types.DATE); index += 1
            OracleUtils.ExecPS(data, ps, index, "TJJSRQ", java.sql.Types.DATE); index += 1
          } else if (DateFormatUtil.STAT_CYCLE_DAY.equals(statCycle)) {
            OracleUtils.ExecPS(data, ps, index, "TJRQ", java.sql.Types.DATE); index += 1
          } else {
            OracleUtils.ExecPS(data, ps, index, "TJRQ", java.sql.Types.VARCHAR); index += 1
          }
          OracleUtils.ExecPS(data, ps, index, "AXDYQXYCDS", java.sql.Types.NUMERIC); index += 1
          OracleUtils.ExecPS(data, ps, index, "AXDYQXSCDS", java.sql.Types.NUMERIC); index += 1
          OracleUtils.ExecPS(data, ps, index, "BXDYQXYCDS", java.sql.Types.NUMERIC); index += 1
          OracleUtils.ExecPS(data, ps, index, "BXDYQXSCDS", java.sql.Types.NUMERIC); index += 1
          OracleUtils.ExecPS(data, ps, index, "CXDYQXYCDS", java.sql.Types.NUMERIC); index += 1
          OracleUtils.ExecPS(data, ps, index, "CXDYQXSCDS", java.sql.Types.NUMERIC); index += 1
          OracleUtils.ExecPS(data, ps, index, "AXDLQXYCDS", java.sql.Types.NUMERIC); index += 1
          OracleUtils.ExecPS(data, ps, index, "AXDLQXSCDS", java.sql.Types.NUMERIC); index += 1
          OracleUtils.ExecPS(data, ps, index, "BXDLQXYCDS", java.sql.Types.NUMERIC); index += 1
          OracleUtils.ExecPS(data, ps, index, "BXDLQXSCDS", java.sql.Types.NUMERIC); index += 1
          OracleUtils.ExecPS(data, ps, index, "CXDLQXYCDS", java.sql.Types.NUMERIC); index += 1
          OracleUtils.ExecPS(data, ps, index, "CXDLQXSCDS", java.sql.Types.NUMERIC); index += 1
          OracleUtils.ExecPS(data, ps, index, "YGZGLQXYCDS", java.sql.Types.NUMERIC); index += 1
          OracleUtils.ExecPS(data, ps, index, "YGZGLQXSCDS", java.sql.Types.NUMERIC); index += 1
          OracleUtils.ExecPS(data, ps, index, "WGZGLQXYCDS", java.sql.Types.NUMERIC); index += 1
          OracleUtils.ExecPS(data, ps, index, "WGZGLQXSCDS", java.sql.Types.NUMERIC); index += 1
          OracleUtils.ExecPS(data, ps, index, "GLYSQXYCDS", java.sql.Types.NUMERIC); index += 1
          OracleUtils.ExecPS(data, ps, index, "GLYSQXSCDS", java.sql.Types.NUMERIC); index += 1

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

  def insertPBBZGatherSucc(df: DataFrame, statDate: String, statCycle: String): Unit = {
    val tableName = DateFormatUtil.tableName("PWYW_DWPBQXCJWZLTJ_BZ", statCycle)
    val errTableName = DateFormatUtil.tableName("ERR_PWYW_DWPBQXCJWZLTJ_BZ", statCycle)

    OracleUtils.detCurDayData(tableName, statCycle, statDate)

    val tjrq = if (DateFormatUtil.STAT_CYCLE_WEEK.equals(statCycle)) "TJKSRQ,TJJSRQ" else "TJRQ"
    val tjrqParam = if (DateFormatUtil.STAT_CYCLE_WEEK.equals(statCycle)) "?,?" else "?"
    val sql =
      s"INSERT INTO ${tableName}\n" +
        s"(DWBM,DWMC,DWJB,SJ_DWBM,SJ_DWMC,PBLX,CNW,PMSPBSL,YCFGPBSL,CJPBSL,${tjrq},AXDYQXYCDS,AXDYQXSCDS,BXDYQXYCDS,BXDYQXSCDS,CXDYQXYCDS,CXDYQXSCDS,AXDLQXYCDS,AXDLQXSCDS,BXDLQXYCDS,BXDLQXSCDS,CXDLQXYCDS,CXDLQXSCDS,YGZGLQXYCDS,YGZGLQXSCDS,WGZGLQXYCDS,WGZGLQXSCDS,GLYSQXYCDS,GLYSQXSCDS)\n" +
        s"VALUES  (?,?,?,?,?,?,?,?,?,?,${tjrqParam},?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)  " //log errors into ${errTableName}('TEST')"

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
          var index: Int = 1

          OracleUtils.ExecPS(data, ps, index, "DWBM", java.sql.Types.VARCHAR); index += 1
          OracleUtils.ExecPS(data, ps, index, "DWMC", java.sql.Types.VARCHAR); index += 1
          OracleUtils.ExecPS(data, ps, index, "DWJB", java.sql.Types.VARCHAR); index += 1
          OracleUtils.ExecPS(data, ps, index, "SJDWBM", java.sql.Types.VARCHAR); index += 1
          OracleUtils.ExecPS(data, ps, index, "SJDWMC", java.sql.Types.VARCHAR); index += 1
          OracleUtils.ExecPS(data, ps, index, "PBLX", java.sql.Types.VARCHAR); index += 1
          OracleUtils.ExecPS(data, ps, index, "CNW", java.sql.Types.VARCHAR); index += 1
          OracleUtils.ExecPS(data, ps, index, "PMSPBSL", java.sql.Types.NUMERIC); index += 1
          OracleUtils.ExecPS(data, ps, index, "YCFGPBSL", java.sql.Types.NUMERIC); index += 1
          OracleUtils.ExecPS(data, ps, index, "CJPBSL", java.sql.Types.NUMERIC); index += 1
          if (DateFormatUtil.STAT_CYCLE_WEEK.equals(statCycle)) {
            OracleUtils.ExecPS(data, ps, index, "TJKSRQ", java.sql.Types.DATE); index += 1
            OracleUtils.ExecPS(data, ps, index, "TJJSRQ", java.sql.Types.DATE); index += 1
          } else if (DateFormatUtil.STAT_CYCLE_DAY.equals(statCycle)) {
            OracleUtils.ExecPS(data, ps, index, "TJRQ", java.sql.Types.DATE); index += 1
          } else {
            OracleUtils.ExecPS(data, ps, index, "TJRQ", java.sql.Types.VARCHAR); index += 1
          }
          OracleUtils.ExecPS(data, ps, index, "AXDYQXYCDS", java.sql.Types.NUMERIC); index += 1
          OracleUtils.ExecPS(data, ps, index, "AXDYQXSCDS", java.sql.Types.NUMERIC); index += 1
          OracleUtils.ExecPS(data, ps, index, "BXDYQXYCDS", java.sql.Types.NUMERIC); index += 1
          OracleUtils.ExecPS(data, ps, index, "BXDYQXSCDS", java.sql.Types.NUMERIC); index += 1
          OracleUtils.ExecPS(data, ps, index, "CXDYQXYCDS", java.sql.Types.NUMERIC); index += 1
          OracleUtils.ExecPS(data, ps, index, "CXDYQXSCDS", java.sql.Types.NUMERIC); index += 1
          OracleUtils.ExecPS(data, ps, index, "AXDLQXYCDS", java.sql.Types.NUMERIC); index += 1
          OracleUtils.ExecPS(data, ps, index, "AXDLQXSCDS", java.sql.Types.NUMERIC); index += 1
          OracleUtils.ExecPS(data, ps, index, "BXDLQXYCDS", java.sql.Types.NUMERIC); index += 1
          OracleUtils.ExecPS(data, ps, index, "BXDLQXSCDS", java.sql.Types.NUMERIC); index += 1
          OracleUtils.ExecPS(data, ps, index, "CXDLQXYCDS", java.sql.Types.NUMERIC); index += 1
          OracleUtils.ExecPS(data, ps, index, "CXDLQXSCDS", java.sql.Types.NUMERIC); index += 1
          OracleUtils.ExecPS(data, ps, index, "YGZGLQXYCDS", java.sql.Types.NUMERIC); index += 1
          OracleUtils.ExecPS(data, ps, index, "YGZGLQXSCDS", java.sql.Types.NUMERIC); index += 1
          OracleUtils.ExecPS(data, ps, index, "WGZGLQXYCDS", java.sql.Types.NUMERIC); index += 1
          OracleUtils.ExecPS(data, ps, index, "WGZGLQXSCDS", java.sql.Types.NUMERIC); index += 1
          OracleUtils.ExecPS(data, ps, index, "GLYSQXYCDS", java.sql.Types.NUMERIC); index += 1
          OracleUtils.ExecPS(data, ps, index, "GLYSQXSCDS", java.sql.Types.NUMERIC); index += 1

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

  def insertPBSSClass(df: DataFrame, statDate: String, statCycle: String): Unit = {
    val tableName = DateFormatUtil.tableName("PWYW_DWPBSJFJTJ", statCycle)
    val errTableName = DateFormatUtil.tableName("ERR_PWYW_DWPBSJFJTJ", statCycle)

    OracleUtils.detCurDayData(tableName, statCycle, statDate)

    val tjrq = if (DateFormatUtil.STAT_CYCLE_WEEK.equals(statCycle)) "TJKSRQ,TJJSRQ" else "TJRQ"
    val tjrqParam = if (DateFormatUtil.STAT_CYCLE_WEEK.equals(statCycle)) "?,?" else "?"
    val sql =
      s"""INSERT INTO ${tableName}
        (DWBM,DWMC,DWJB,SJDWBM,SJDWMC,
        CNW,PBLX,PBXB,EDRLDJ,${tjrq},
        GJBM,SJDJ,PMSPBSL,CJPBSL,GZTS,
        GZZB,GZCS,GZSC)
        VALUES  (?,?,?,?,?,
        ?,?,?,?,${tjrqParam},
        ?,?,?,?,?,
        ?,?,?)"""

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
          var index: Int = 1
          OracleUtils.ExecPS(data, ps, index, "DWBM", java.sql.Types.VARCHAR); index += 1
          OracleUtils.ExecPS(data, ps, index, "DWMC", java.sql.Types.VARCHAR); index += 1
          OracleUtils.ExecPS(data, ps, index, "DWJB", java.sql.Types.VARCHAR); index += 1
          OracleUtils.ExecPS(data, ps, index, "SJDWBM", java.sql.Types.VARCHAR); index += 1
          OracleUtils.ExecPS(data, ps, index, "SJDWMC", java.sql.Types.VARCHAR); index += 1

          OracleUtils.ExecPS(data, ps, index, "CNW", java.sql.Types.VARCHAR); index += 1
          OracleUtils.ExecPS(data, ps, index, "PBLX", java.sql.Types.VARCHAR); index += 1
          OracleUtils.ExecPS(data, ps, index, "PBXB", java.sql.Types.VARCHAR); index += 1
          OracleUtils.ExecPS(data, ps, index, "EDRLDJ", java.sql.Types.NUMERIC); index += 1
          if (DateFormatUtil.STAT_CYCLE_WEEK.equals(statCycle)) {
            OracleUtils.ExecPS(data, ps, index, "TJKSRQ", java.sql.Types.DATE); index += 1
            OracleUtils.ExecPS(data, ps, index, "TJJSRQ", java.sql.Types.DATE); index += 1
          } else if (DateFormatUtil.STAT_CYCLE_DAY.equals(statCycle)) {
            OracleUtils.ExecPS(data, ps, index, "TJRQ", java.sql.Types.DATE); index += 1
          } else {
            OracleUtils.ExecPS(data, ps, index, "TJRQ", java.sql.Types.VARCHAR); index += 1
          }

          OracleUtils.ExecPS(data, ps, index, "GJBM", java.sql.Types.VARCHAR); index += 1
          OracleUtils.ExecPS(data, ps, index, "SJDJ", java.sql.Types.VARCHAR); index += 1
          OracleUtils.ExecPS(data, ps, index, "PMSPBSL", java.sql.Types.NUMERIC); index += 1
          OracleUtils.ExecPS(data, ps, index, "CJPBSL", java.sql.Types.NUMERIC); index += 1
          OracleUtils.ExecPS(data, ps, index, "GZTS", java.sql.Types.NUMERIC); index += 1

          OracleUtils.ExecPS(data, ps, index, "GZZB", java.sql.Types.NUMERIC); index += 1
          OracleUtils.ExecPS(data, ps, index, "GZCS", java.sql.Types.NUMERIC); index += 1
          OracleUtils.ExecPS(data, ps, index, "GZSC", java.sql.Types.NUMERIC); index += 1

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