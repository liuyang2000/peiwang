package com.bmsoft.impl

import java.sql.Connection
import java.sql.DriverManager
import java.sql.PreparedStatement

import org.apache.spark.annotation.Experimental
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.countDistinct
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.functions.max
import org.apache.spark.sql.functions.min
import org.apache.spark.sql.functions.sum
import org.apache.spark.sql.hive.HiveContext

import com.bmsoft.bean.DataBean
import com.bmsoft.bean.RunStatusBean
import com.bmsoft.bean.OracleRunStatusBean
import com.bmsoft.util.JdbcConnUtil
import com.bmsoft.util.DateFormatUtil
import org.apache.spark.sql.functions._

/**
 * 馈线统计实现类
 * @author mathsyang
 */
class KXStatImpl(hc: HiveContext, statDate: String, statCycle: String) {
  def statSSExcept: DataFrame = {
    import hc.implicits._
    val dateFrom = DateFormatUtil.dateFrom(statDate, statCycle)
    val dateTo = DateFormatUtil.dateTo(dateFrom, statCycle)

    val weekDateTo = DateFormatUtil.dateDay(dateTo, 1)
    val tjrq = if (DateFormatUtil.STAT_CYCLE_WEEK.equals(statCycle)) s"'${dateFrom}' TJKSRQ, '${weekDateTo}' TJJSRQ\n"
    else s"'${statDate}' TJRQ\n"

    val sql = s"""SELECT t.OBJ_ID,
                         t.SSDS DWBM,
                         t2.PMS_DWCJ DWJB,
                         t.CNW,
                         t.XLXZ,
                         t3.YX_XLBS,
                         t4.XLID,
                         t5.GJBM,
                         t5.DQGJFSCS,
                         t5.DQGJFSSC,
                         t5.SJDJ,
                         ${tjrq}
                FROM 
                  (SELECT OBJ_ID,
                         SSDS,
                         XLXZ,
                         CASE WHEN SFNW = '1' THEN '3'ELSE '2' END CNW
                  FROM pwyw_arch.T_SB_ZWYC_XL
                  WHERE SFNW is NOT null
                  union all
                  SELECT OBJ_ID,
                         b.sjdwid SSDS,
                         XLXZ,
                         CASE WHEN SFNW = '1' THEN '3'ELSE '2' END CNW
                  FROM pwyw_arch.t_sb_zwyc_xl a JOIN pwyw_arch.ST_PMS_YX_DW b ON (a.SSDS = b.pms_dwid)
                  WHERE SFNW is NOT null) t
                LEFT JOIN pwyw_arch.ST_PMS_YX_DW t2
                    ON (t.ssds = t2.pms_dwid)
                LEFT JOIN pwyw_arch.ST_BDZ_XL t3
                    ON (t.OBJ_ID = t3.pms_xlbs)
                LEFT JOIN (select distinct XLID from pwyw_arch.E_DMS_XL_CURVE where dt >= '${dateFrom}' AND dt < '${dateTo}') t4
                    ON (t.obj_id = t4.xlid)
                LEFT JOIN (select * from pwyw.PWYW_KXYDYC where dt >= '${dateFrom}' AND dt < '${dateTo}' and dqgjfssj >= '${dateFrom}' and dqgjfssj < '${dateTo}') t5
                    ON (t.OBJ_ID = t5.xlid)"""
    val joinData = hc.sql(sql)

    //    val joinData = querySSJoinData(hc, statDate, dateFrom, dateTo, statCycle).distinct()

    val dfGroup = if (DateFormatUtil.STAT_CYCLE_WEEK.equals(statCycle)) joinData.groupBy("DWBM", "DWJB", "XLXZ", "CNW", "TJKSRQ", "TJJSRQ")
    else joinData.groupBy("DWBM", "DWJB", "XLXZ", "CNW", "TJRQ")
    val df = dfGroup.agg(
      countDistinct("OBJ_ID") as ("PMSKXSL"),
      countDistinct("YX_XLBS") as ("YCFGKXSL"),
      countDistinct("XLID") as ("CJKXSL"),

      countDistinct(expr("case when GJBM = '00110' then OBJ_ID end")) as ("GDYTS"),
      sum(expr("case when GJBM = '00110' then DQGJFSCS end")) as ("GDYCS"),
      sum(expr("case when GJBM = '00110' then DQGJFSSC end")) as ("GDYSC"),
      countDistinct(expr("case when GJBM = '00111' then OBJ_ID end")) as ("DDYTS"),
      sum(expr("case when GJBM = '00111' then DQGJFSCS end")) as ("DDYCS"),
      sum(expr("case when GJBM = '00111' then DQGJFSSC end")) as ("DDYSC"),
      countDistinct(expr("case when GJBM = '00112' then OBJ_ID end")) as ("DLSXBPHTS"),
      sum(expr("case when GJBM = '00112' then DQGJFSCS end")) as ("DLSXBPHCS"),
      sum(expr("case when GJBM = '00112' then DQGJFSSC end")) as ("DLSXBPHSC"),
      countDistinct(expr("case when GJBM = '00115' then OBJ_ID end")) as ("GZTS"),
      sum(expr("case when GJBM = '00115' then DQGJFSCS end")) as ("GZCS"),
      sum(expr("case when GJBM = '00115' then DQGJFSSC end")) as ("GZSC"),
      countDistinct(expr("case when GJBM = '00116' then OBJ_ID end")) as ("GLYSYCTS"),
      sum(expr("case when GJBM = '00116' then DQGJFSCS end")) as ("GLYSYCCS"),
      sum(expr("case when GJBM = '00116' then DQGJFSSC end")) as ("GLYSYCSC"),
      countDistinct(expr("case when GJBM = '00118' then OBJ_ID end")) as ("DYSXBPHTS"),
      sum(expr("case when GJBM = '00118' then DQGJFSCS end")) as ("DYSXBPHCS"),
      sum(expr("case when GJBM = '00118' then DQGJFSSC end")) as ("DYSXBPHSC"),
      countDistinct(expr("case when GJBM = '00130' then OBJ_ID end")) as ("AGZTS"),
      sum(expr("case when GJBM = '00130' then DQGJFSCS end")) as ("AGZCS"),
      sum(expr("case when GJBM = '00130' then DQGJFSSC end")) as ("AGZSC"),
      countDistinct(expr("case when GJBM = '00131' then OBJ_ID end")) as ("BGZTS"),
      sum(expr("case when GJBM = '00131' then DQGJFSCS end")) as ("BGZCS"),
      sum(expr("case when GJBM = '00131' then DQGJFSSC end")) as ("BGZSC"),
      countDistinct(expr("case when GJBM = '00132' then OBJ_ID end")) as ("CGZTS"),
      sum(expr("case when GJBM = '00132' then DQGJFSCS end")) as ("CGZCS"),
      sum(expr("case when GJBM = '00132' then DQGJFSSC end")) as ("CGZSC"),
      countDistinct(expr("case when GJBM = '00133' then OBJ_ID end")) as ("AZZTS"),
      sum(expr("case when GJBM = '00133' then DQGJFSCS end")) as ("AZZCS"),
      sum(expr("case when GJBM = '00133' then DQGJFSSC end")) as ("AZZSC"),
      countDistinct(expr("case when GJBM = '00134' then OBJ_ID end")) as ("BZZTS"),
      sum(expr("case when GJBM = '00134' then DQGJFSCS end")) as ("BZZCS"),
      sum(expr("case when GJBM = '00134' then DQGJFSSC end")) as ("BZZSC"),
      countDistinct(expr("case when GJBM = '00135' then OBJ_ID end")) as ("CZZTS"),
      sum(expr("case when GJBM = '00135' then DQGJFSCS end")) as ("CZZCS"),
      sum(expr("case when GJBM = '00135' then DQGJFSSC end")) as ("CZZSC"),
      //      countDistinct(expr("case when GJBM = '00136' then OBJ_ID end")) as ("AQZTS"),
      //      sum(expr("case when GJBM = '00136' then DQGJFSCS end")) as ("AQZCS"),
      //      sum(expr("case when GJBM = '00136' then DQGJFSSC end")) as ("AQZSC"),
      //      countDistinct(expr("case when GJBM = '00137' then OBJ_ID end")) as ("BQZTS"),
      //      sum(expr("case when GJBM = '00137' then DQGJFSCS end")) as ("BQZCS"),
      //      sum(expr("case when GJBM = '00137' then DQGJFSSC end")) as ("BQZSC"),
      //      countDistinct(expr("case when GJBM = '00138' then OBJ_ID end")) as ("CQZTS"),
      //      sum(expr("case when GJBM = '00138' then DQGJFSCS end")) as ("CQZCS"),
      //      sum(expr("case when GJBM = '00138' then DQGJFSSC end")) as ("CQZSC"),
      countDistinct(expr("case when GJBM = '00139' then OBJ_ID end")) as ("ZZTS"),
      sum(expr("case when GJBM = '00139' then DQGJFSCS end")) as ("ZZCS"),
      sum(expr("case when GJBM = '00139' then DQGJFSSC end")) as ("ZZSC"),
      countDistinct(expr("case when GJBM = '0013A' then OBJ_ID end")) as ("QZTS"),
      sum(expr("case when GJBM = '0013A' then DQGJFSCS end")) as ("QZCS"),
      sum(expr("case when GJBM = '0013A' then DQGJFSSC end")) as ("QZSC"),

      countDistinct(expr("case when GJBM in ('00110','00111','00118') then OBJ_ID end")) as ("DYYCTS"),
      countDistinct(expr("case when GJBM in ('00130','00131','00132','00133','00134','00135','00136','00137','00138','00139','0013A') then OBJ_ID end")) as ("FZYCTS"),
      countDistinct(expr("case when GJBM in ('00130','00131','00132','00133','00134','00135','00136','00137','00138','00139','0013A') and SJDJ = '1' then OBJ_ID end")) as ("FZL1TS"),
      countDistinct(expr("case when GJBM in ('00130','00131','00132','00133','00134','00135','00136','00137','00138','00139','0013A') and SJDJ = '2' then OBJ_ID end")) as ("FZL2TS"),
      countDistinct(expr("case when GJBM in ('00130','00131','00132','00133','00134','00135','00136','00137','00138','00139','0013A') and SJDJ = '3' then OBJ_ID end")) as ("FZL3TS"),
      countDistinct(expr("case when GJBM in ('00130','00131','00132','00133','00134','00135','00136','00137','00138','00139','0013A') and SJDJ = '4' then OBJ_ID end")) as ("FZL4TS"))
    val res = if (DateFormatUtil.STAT_CYCLE_WEEK.equals(statCycle))
      df.select($"DWBM",
        $"DWJB",
        $"XLXZ",
        $"CNW",
        $"TJKSRQ", $"TJJSRQ",
        $"PMSKXSL",
        $"YCFGKXSL",
        $"CJKXSL",
        $"GDYTS",
        expr("round(GDYTS*100/PMSKXSL,2) GDYZB"),
        $"GDYCS",
        $"GDYSC",
        $"DDYTS",
        expr("round(DDYTS*100/PMSKXSL,2) DDYZB"),
        $"DDYCS",
        $"DDYSC",
        $"DLSXBPHTS",
        expr("round(DLSXBPHTS*100/PMSKXSL,2) DLSXBPHZB"),
        $"DLSXBPHCS",
        $"DLSXBPHSC",
        $"GZTS",
        expr("round(GZTS*100/PMSKXSL,2) GZZB"),
        $"GZCS",
        $"GZSC",
        $"GLYSYCTS",
        expr("round(GLYSYCTS*100/PMSKXSL,2) GLYSYCZB"),
        $"GLYSYCCS",
        $"GLYSYCSC",
        $"DYSXBPHTS",
        expr("round(DYSXBPHTS*100/PMSKXSL,2) DYSXBPHZB"),
        $"DYSXBPHCS",
        $"DYSXBPHSC",
        $"AGZTS",
        expr("round(AGZTS*100/PMSKXSL,2) AGZZB"),
        $"AGZCS",
        $"AGZSC",
        $"BGZTS",
        expr("round(BGZTS*100/PMSKXSL,2) BGZZB"),
        $"BGZCS",
        $"BGZSC",
        $"CGZTS",
        expr("round(CGZTS*100/PMSKXSL,2) CGZZB"),
        $"CGZCS",
        $"CGZSC",
        $"AZZTS",
        expr("round(AZZTS*100/PMSKXSL,2) AZZZB"),
        $"AZZCS",
        $"AZZSC",
        $"BZZTS",
        expr("round(BZZTS*100/PMSKXSL,2) BZZZB"),
        $"BZZCS",
        $"BZZSC",
        $"CZZTS",
        expr("round(CZZTS*100/PMSKXSL,2) CZZZB"),
        $"CZZCS",
        $"CZZSC",
        //        $"AQZTS",
        //        expr("round(AQZTS*100/PMSKXSL,2) AQZZB"),
        //        $"AQZCS",
        //        $"AQZSC",
        //        $"BQZTS",
        //        expr("round(BQZTS*100/PMSKXSL,2) BQZZB"),
        //        $"BQZCS",
        //        $"BQZSC",
        //        $"CQZTS",
        //        expr("round(CQZTS*100/PMSKXSL,2) CQZZB"),
        //        $"CQZCS",
        //        $"CQZSC",
        $"ZZTS",
        expr("round(ZZTS*100/PMSKXSL,2) ZZZB"),
        $"ZZCS",
        $"ZZSC",
        $"QZTS",
        expr("round(QZTS*100/PMSKXSL,2) QZZB"),
        $"QZCS",
        $"QZSC",
        $"DYYCTS",
        expr("round(DYYCTS*100/PMSKXSL,2) DYYCZB"),
        $"FZYCTS",
        expr("round(FZYCTS*100/PMSKXSL,2) FZYCZB"),
        $"FZL1TS",
        $"FZL2TS",
        $"FZL3TS",
        $"FZL4TS")//,
//        expr("null FZL5TS"),
//        expr("null FZL6TS"),
//        expr("null POWEROFF_PIONT_CS"))
    else
      df.select($"DWBM",
        $"DWJB",
        $"XLXZ",
        $"CNW",
        $"TJRQ",
        $"PMSKXSL",
        $"YCFGKXSL",
        $"CJKXSL",
        $"GDYTS",
        expr("round(GDYTS*100/PMSKXSL,2) GDYZB"),
        $"GDYCS",
        $"GDYSC",
        $"DDYTS",
        expr("round(DDYTS*100/PMSKXSL,2) DDYZB"),
        $"DDYCS",
        $"DDYSC",
        $"DLSXBPHTS",
        expr("round(DLSXBPHTS*100/PMSKXSL,2) DLSXBPHZB"),
        $"DLSXBPHCS",
        $"DLSXBPHSC",
        $"GZTS",
        expr("round(GZTS*100/PMSKXSL,2) GZZB"),
        $"GZCS",
        $"GZSC",
        $"GLYSYCTS",
        expr("round(GLYSYCTS*100/PMSKXSL,2) GLYSYCZB"),
        $"GLYSYCCS",
        $"GLYSYCSC",
        $"DYSXBPHTS",
        expr("round(DYSXBPHTS*100/PMSKXSL,2) DYSXBPHZB"),
        $"DYSXBPHCS",
        $"DYSXBPHSC",
        $"AGZTS",
        expr("round(AGZTS*100/PMSKXSL,2) AGZZB"),
        $"AGZCS",
        $"AGZSC",
        $"BGZTS",
        expr("round(BGZTS*100/PMSKXSL,2) BGZZB"),
        $"BGZCS",
        $"BGZSC",
        $"CGZTS",
        expr("round(CGZTS*100/PMSKXSL,2) CGZZB"),
        $"CGZCS",
        $"CGZSC",
        $"AZZTS",
        expr("round(AZZTS*100/PMSKXSL,2) AZZZB"),
        $"AZZCS",
        $"AZZSC",
        $"BZZTS",
        expr("round(BZZTS*100/PMSKXSL,2) BZZZB"),
        $"BZZCS",
        $"BZZSC",
        $"CZZTS",
        expr("round(CZZTS*100/PMSKXSL,2) CZZZB"),
        $"CZZCS",
        $"CZZSC",
        //        $"AQZTS",
        //        expr("round(AQZTS*100/PMSKXSL,2) AQZZB"),
        //        $"AQZCS",
        //        $"AQZSC",
        //        $"BQZTS",
        //        expr("round(BQZTS*100/PMSKXSL,2) BQZZB"),
        //        $"BQZCS",
        //        $"BQZSC",
        //        $"CQZTS",
        //        expr("round(CQZTS*100/PMSKXSL,2) CQZZB"),
        //        $"CQZCS",
        //        $"CQZSC",
        $"ZZTS",
        expr("round(ZZTS*100/PMSKXSL,2) ZZZB"),
        $"ZZCS",
        $"ZZSC",
        $"QZTS",
        expr("round(QZTS*100/PMSKXSL,2) QZZB"),
        $"QZCS",
        $"QZSC",
        $"DYYCTS",
        expr("round(DYYCTS*100/PMSKXSL,2) DYYCZB"),
        $"FZYCTS",
        expr("round(FZYCTS*100/PMSKXSL,2) FZYCZB"),
        $"FZL1TS",
        $"FZL2TS",
        $"FZL3TS",
        $"FZL4TS")//,
//        expr("null FZL5TS"),
//        expr("null FZL6TS"),
//        expr("null POWEROFF_PIONT_CS"))
    res
  }

  def statSSGatherSuccRateWeekAndMore: DataFrame = {
    import hc.implicits._
    val dateFrom = DateFormatUtil.dateFrom(statDate, statCycle)
    val dateTo = DateFormatUtil.dateTo(dateFrom, statCycle)

    val ycds = DateFormatUtil.statDays(dateFrom, dateTo, statCycle) * 96l

    val weekDateTo = DateFormatUtil.dateDay(dateTo, 1)
    val tjrq = if (DateFormatUtil.STAT_CYCLE_WEEK.equals(statCycle)) s"'${dateFrom}' TJKSRQ, '${weekDateTo}' TJJSRQ\n"
    else s"'${statDate}' TJRQ\n"

    val archSql = s"""select DWBM,
       DWMC,
       DWJB,
       CNW,
       SJ_DWBM,
       SJ_DWMC,
       PMSKXSL,
       XLXZ,
       YCFGKXSL,
       CJKXSL,
       ${tjrq}
  from pwyw_dwkxqxcjwzltj_ss_r
 where tjrq = (select max(tjrq) from pwyw_dwpbqxcjwzltj_ss_r)"""

    val arch = hc.read.jdbc(JdbcConnUtil.url, s"(${archSql})", JdbcConnUtil.connProp)

    val dataSql = s"""select DWBM,
       CNW,
       XLXZ,
       AXDYQXYCDS,
       AXDYQXSCDS,
       BXDYQXYCDS,
       BXDYQXSCDS,
       CXDYQXYCDS,
       CXDYQXSCDS,
       AXDLQXYCDS,
       AXDLQXSCDS,
       BXDLQXYCDS,
       BXDLQXSCDS,
       CXDLQXYCDS,
       CXDLQXSCDS,
       GLYSQXYCDS,
       GLYSQXSCDS,
       YGZGLQXYCDS,
       YGZGLQXSCDS,
       WGZGLQXYCDS,
       WGZGLQXSCDS
  from pwyw_dwkxqxcjwzltj_ss_r
 where tjrq >= to_date('${dateFrom}', 'yyyymmdd')
   and tjrq < to_date('${dateTo}', 'yyyymmdd')"""

    val data = hc.read.jdbc(JdbcConnUtil.url, s"(${dataSql})", JdbcConnUtil.connProp)

    val dfGroup = data.groupBy("DWBM", "XLXZ", "CNW")
    val df = dfGroup.agg(
      sum("AXDYQXYCDS") as "AXDYQXYCDS",
      sum("AXDYQXSCDS") as "AXDYQXSCDS",
      sum("BXDYQXYCDS") as "BXDYQXYCDS",
      sum("BXDYQXSCDS") as "BXDYQXSCDS",
      sum("CXDYQXYCDS") as "CXDYQXYCDS",
      sum("CXDYQXSCDS") as "CXDYQXSCDS",
      sum("AXDLQXYCDS") as "AXDLQXYCDS",
      sum("AXDLQXSCDS") as "AXDLQXSCDS",
      sum("BXDLQXYCDS") as "BXDLQXYCDS",
      sum("BXDLQXSCDS") as "BXDLQXSCDS",
      sum("CXDLQXYCDS") as "CXDLQXYCDS",
      sum("CXDLQXSCDS") as "CXDLQXSCDS",
      sum("GLYSQXYCDS") as "GLYSQXYCDS",
      sum("GLYSQXSCDS") as "GLYSQXSCDS",
      sum("YGZGLQXYCDS") as "YGZGLQXYCDS",
      sum("YGZGLQXSCDS") as "YGZGLQXSCDS",
      sum("WGZGLQXYCDS") as "WGZGLQXYCDS",
      sum("WGZGLQXSCDS") as "WGZGLQXSCDS")
    val res = arch.join(data, Seq("DWBM", "XLXZ", "CNW"))
    res
  }

  def statSSGatherSuccRate: DataFrame = {
    val df = if (DateFormatUtil.STAT_CYCLE_DAY.equals(statCycle)) {
      statSSGatherSuccRateDay
    } else {
      statSSGatherSuccRateWeekAndMore
    }
    df
  }

  def statBZExcept: DataFrame = {
    import hc.implicits._
    val dateFrom = DateFormatUtil.dateFrom(statDate, statCycle)
    val dateTo = DateFormatUtil.dateTo(dateFrom, statCycle)

    val weekDateTo = DateFormatUtil.dateDay(dateTo, 1)
    val tjrq = if (DateFormatUtil.STAT_CYCLE_WEEK.equals(statCycle)) s"'${dateFrom}' TJKSRQ, '${weekDateTo}' TJJSRQ\n"
    else s"'${statDate}' TJRQ\n"

    val sql = s"""SELECT t.OBJ_ID,
                         t.WHBZ DWBM,
                         t2.PMS_DWCJ DWJB,
                         t.CNW,
                         t.XLXZ,
                         t4.XLID,
                         t3.YX_XLBS,
                         t4.XLID,
                         t5.GJBM,
                         t5.DQGJFSCS,
                         t5.DQGJFSSC,
                         t5.SJDJ,
                         ${tjrq}
                FROM 
                  (SELECT OBJ_ID,WHBZ,xlxz,CASE WHEN SFNW = '1' THEN '3'ELSE '2' END CNW
                  FROM pwyw_arch.T_SB_ZWYC_XL
                  WHERE SFNW is NOT null
                  UNION all
                  SELECT OBJ_ID,b.sjdwid WHBZ,xlxz,CASE WHEN SFNW = '1' THEN '3'ELSE '2' END CNW
                  FROM pwyw_arch.T_SB_ZWYC_XL a JOIN pwyw_arch.ST_PMS_YX_DW b ON (a.WHBZ = b.pms_dwid)
                  WHERE SFNW is NOT null) t
                LEFT JOIN pwyw_arch.ST_PMS_YX_DW t2
                    ON (t.WHBZ = t2.PMS_DWID)
                LEFT JOIN pwyw_arch.ST_BDZ_XL t3
                    ON (t.OBJ_ID = t3.PMS_XLBS)
                LEFT JOIN (select distinct XLID from pwyw_arch.E_DMS_XL_CURVE where dt >= '${dateFrom}' AND dt < '${dateTo}') t4
                    ON (t.obj_id = t4.xlid)
                LEFT JOIN (select * from pwyw.PWYW_KXYDYC where dt >= '${dateFrom}' AND dt < '${dateTo}' and dqgjfssj >= '${dateFrom}' and dqgjfssj < '${dateTo}') t5
                    ON (t.OBJ_ID = t5.xlid)"""
    val joinData = hc.sql(sql)
    //    val joinData = queryBZJoinData(hc, statDate, dateFrom, dateTo, statCycle).distinct()

    val dfGroup = if (DateFormatUtil.STAT_CYCLE_WEEK.equals(statCycle)) joinData.groupBy("DWBM", "DWJB", "XLXZ", "CNW", "TJKSRQ", "TJJSRQ")
    else joinData.groupBy("DWBM", "DWJB", "XLXZ", "CNW", "TJRQ")
    val df = dfGroup.agg(
      countDistinct("OBJ_ID") as ("PMSKXSL"),
      countDistinct("YX_XLBS") as ("YCFGKXSL"),
      countDistinct("XLID") as ("CJKXSL"),

      countDistinct(expr("case when GJBM = '00110' then OBJ_ID end")) as ("GDYTS"),
      sum(expr("case when GJBM = '00110' then DQGJFSCS end")) as ("GDYCS"),
      sum(expr("case when GJBM = '00110' then DQGJFSSC end")) as ("GDYSC"),
      countDistinct(expr("case when GJBM = '00111' then OBJ_ID end")) as ("DDYTS"),
      sum(expr("case when GJBM = '00111' then DQGJFSCS end")) as ("DDYCS"),
      sum(expr("case when GJBM = '00111' then DQGJFSSC end")) as ("DDYSC"),
      countDistinct(expr("case when GJBM = '00112' then OBJ_ID end")) as ("DLSXBPHTS"),
      sum(expr("case when GJBM = '00112' then DQGJFSCS end")) as ("DLSXBPHCS"),
      sum(expr("case when GJBM = '00112' then DQGJFSSC end")) as ("DLSXBPHSC"),
      countDistinct(expr("case when GJBM = '00115' then OBJ_ID end")) as ("GZTS"),
      sum(expr("case when GJBM = '00115' then DQGJFSCS end")) as ("GZCS"),
      sum(expr("case when GJBM = '00115' then DQGJFSSC end")) as ("GZSC"),
      countDistinct(expr("case when GJBM = '00116' then OBJ_ID end")) as ("GLYSYCTS"),
      sum(expr("case when GJBM = '00116' then DQGJFSCS end")) as ("GLYSYCCS"),
      sum(expr("case when GJBM = '00116' then DQGJFSSC end")) as ("GLYSYCSC"),
      countDistinct(expr("case when GJBM = '00118' then OBJ_ID end")) as ("DYSXBPHTS"),
      sum(expr("case when GJBM = '00118' then DQGJFSCS end")) as ("DYSXBPHCS"),
      sum(expr("case when GJBM = '00118' then DQGJFSSC end")) as ("DYSXBPHSC"),
      countDistinct(expr("case when GJBM = '00130' then OBJ_ID end")) as ("AGZTS"),
      sum(expr("case when GJBM = '00130' then DQGJFSCS end")) as ("AGZCS"),
      sum(expr("case when GJBM = '00130' then DQGJFSSC end")) as ("AGZSC"),
      countDistinct(expr("case when GJBM = '00131' then OBJ_ID end")) as ("BGZTS"),
      sum(expr("case when GJBM = '00131' then DQGJFSCS end")) as ("BGZCS"),
      sum(expr("case when GJBM = '00131' then DQGJFSSC end")) as ("BGZSC"),
      countDistinct(expr("case when GJBM = '00132' then OBJ_ID end")) as ("CGZTS"),
      sum(expr("case when GJBM = '00132' then DQGJFSCS end")) as ("CGZCS"),
      sum(expr("case when GJBM = '00132' then DQGJFSSC end")) as ("CGZSC"),
      countDistinct(expr("case when GJBM = '00133' then OBJ_ID end")) as ("AZZTS"),
      sum(expr("case when GJBM = '00133' then DQGJFSCS end")) as ("AZZCS"),
      sum(expr("case when GJBM = '00133' then DQGJFSSC end")) as ("AZZSC"),
      countDistinct(expr("case when GJBM = '00134' then OBJ_ID end")) as ("BZZTS"),
      sum(expr("case when GJBM = '00134' then DQGJFSCS end")) as ("BZZCS"),
      sum(expr("case when GJBM = '00134' then DQGJFSSC end")) as ("BZZSC"),
      countDistinct(expr("case when GJBM = '00135' then OBJ_ID end")) as ("CZZTS"),
      sum(expr("case when GJBM = '00135' then DQGJFSCS end")) as ("CZZCS"),
      sum(expr("case when GJBM = '00135' then DQGJFSSC end")) as ("CZZSC"),
      //      countDistinct(expr("case when GJBM = '00136' then OBJ_ID end")) as ("AQZTS"),
      //      sum(expr("case when GJBM = '00136' then DQGJFSCS end")) as ("AQZCS"),
      //      sum(expr("case when GJBM = '00136' then DQGJFSSC end")) as ("AQZSC"),
      //      countDistinct(expr("case when GJBM = '00137' then OBJ_ID end")) as ("BQZTS"),
      //      sum(expr("case when GJBM = '00137' then DQGJFSCS end")) as ("BQZCS"),
      //      sum(expr("case when GJBM = '00137' then DQGJFSSC end")) as ("BQZSC"),
      //      countDistinct(expr("case when GJBM = '00138' then OBJ_ID end")) as ("CQZTS"),
      //      sum(expr("case when GJBM = '00138' then DQGJFSCS end")) as ("CQZCS"),
      //      sum(expr("case when GJBM = '00138' then DQGJFSSC end")) as ("CQZSC"),
      countDistinct(expr("case when GJBM = '00139' then OBJ_ID end")) as ("ZZTS"),
      sum(expr("case when GJBM = '00139' then DQGJFSCS end")) as ("ZZCS"),
      sum(expr("case when GJBM = '00139' then DQGJFSSC end")) as ("ZZSC"),
      countDistinct(expr("case when GJBM = '0013A' then OBJ_ID end")) as ("QZTS"),
      sum(expr("case when GJBM = '0013A' then DQGJFSCS end")) as ("QZCS"),
      sum(expr("case when GJBM = '0013A' then DQGJFSSC end")) as ("QZSC"),

      countDistinct(expr("case when GJBM in ('00110','00111','00118') then OBJ_ID end")) as ("DYYCTS"),
      countDistinct(expr("case when GJBM in ('00130','00131','00132','00133','00134','00135','00136','00137','00138','00139','0013A') then OBJ_ID end")) as ("FZYCTS"),
      countDistinct(expr("case when GJBM in ('00130','00131','00132','00133','00134','00135','00136','00137','00138','00139','0013A') and SJDJ = '1' then OBJ_ID end")) as ("FZL1TS"),
      countDistinct(expr("case when GJBM in ('00130','00131','00132','00133','00134','00135','00136','00137','00138','00139','0013A') and SJDJ = '2' then OBJ_ID end")) as ("FZL2TS"),
      countDistinct(expr("case when GJBM in ('00130','00131','00132','00133','00134','00135','00136','00137','00138','00139','0013A') and SJDJ = '3' then OBJ_ID end")) as ("FZL3TS"),
      countDistinct(expr("case when GJBM in ('00130','00131','00132','00133','00134','00135','00136','00137','00138','00139','0013A') and SJDJ = '4' then OBJ_ID end")) as ("FZL4TS"))
    val res = if (DateFormatUtil.STAT_CYCLE_WEEK.equals(statCycle))
      df.select($"DWBM",
        $"DWJB",
        $"XLXZ",
        $"CNW",
        $"TJKSRQ", $"TJJSRQ",
        $"PMSKXSL",
        $"YCFGKXSL",
        $"CJKXSL",
        $"GDYTS",
        expr("round(GDYTS*100/PMSKXSL,2) GDYZB"),
        $"GDYCS",
        $"GDYSC",
        $"DDYTS",
        expr("round(DDYTS*100/PMSKXSL,2) DDYZB"),
        $"DDYCS",
        $"DDYSC",
        $"DLSXBPHTS",
        expr("round(DLSXBPHTS*100/PMSKXSL,2) DLSXBPHZB"),
        $"DLSXBPHCS",
        $"DLSXBPHSC",
        $"GZTS",
        expr("round(GZTS*100/PMSKXSL,2) GZZB"),
        $"GZCS",
        $"GZSC",
        $"GLYSYCTS",
        expr("round(GLYSYCTS*100/PMSKXSL,2) GLYSYCZB"),
        $"GLYSYCCS",
        $"GLYSYCSC",
        $"DYSXBPHTS",
        expr("round(DYSXBPHTS*100/PMSKXSL,2) DYSXBPHZB"),
        $"DYSXBPHCS",
        $"DYSXBPHSC",
        $"AGZTS",
        expr("round(AGZTS*100/PMSKXSL,2) AGZZB"),
        $"AGZCS",
        $"AGZSC",
        $"BGZTS",
        expr("round(BGZTS*100/PMSKXSL,2) BGZZB"),
        $"BGZCS",
        $"BGZSC",
        $"CGZTS",
        expr("round(CGZTS*100/PMSKXSL,2) CGZZB"),
        $"CGZCS",
        $"CGZSC",
        $"AZZTS",
        expr("round(AZZTS*100/PMSKXSL,2) AZZZB"),
        $"AZZCS",
        $"AZZSC",
        $"BZZTS",
        expr("round(BZZTS*100/PMSKXSL,2) BZZZB"),
        $"BZZCS",
        $"BZZSC",
        $"CZZTS",
        expr("round(CZZTS*100/PMSKXSL,2) CZZZB"),
        $"CZZCS",
        $"CZZSC",
        //        $"AQZTS",
        //        expr("round(AQZTS*100/PMSKXSL,2) AQZZB"),
        //        $"AQZCS",
        //        $"AQZSC",
        //        $"BQZTS",
        //        expr("round(BQZTS*100/PMSKXSL,2) BQZZB"),
        //        $"BQZCS",
        //        $"BQZSC",
        //        $"CQZTS",
        //        expr("round(CQZTS*100/PMSKXSL,2) CQZZB"),
        //        $"CQZCS",
        //        $"CQZSC",
        $"ZZTS",
        expr("round(ZZTS*100/PMSKXSL,2) ZZZB"),
        $"ZZCS",
        $"ZZSC",
        $"QZTS",
        expr("round(QZTS*100/PMSKXSL,2) QZZB"),
        $"QZCS",
        $"QZSC",
        $"DYYCTS",
        expr("round(DYYCTS*100/PMSKXSL,2) DYYCZB"),
        $"FZYCTS",
        expr("round(FZYCTS*100/PMSKXSL,2) FZYCZB"),
        $"FZL1TS",
        $"FZL2TS",
        $"FZL3TS",
        $"FZL4TS")//,
//        expr("null FZL5TS"),
//        expr("null FZL6TS"),
//        expr("null POWEROFF_PIONT_CS"))
    else
      df.select($"DWBM",
        $"DWJB",
        $"XLXZ",
        $"CNW",
        $"TJRQ",
        $"PMSKXSL",
        $"YCFGKXSL",
        $"CJKXSL",
        $"GDYTS",
        expr("round(GDYTS*100/PMSKXSL,2) GDYZB"),
        $"GDYCS",
        $"GDYSC",
        $"DDYTS",
        expr("round(DDYTS*100/PMSKXSL,2) DDYZB"),
        $"DDYCS",
        $"DDYSC",
        $"DLSXBPHTS",
        expr("round(DLSXBPHTS*100/PMSKXSL,2) DLSXBPHZB"),
        $"DLSXBPHCS",
        $"DLSXBPHSC",
        $"GZTS",
        expr("round(GZTS*100/PMSKXSL,2) GZZB"),
        $"GZCS",
        $"GZSC",
        $"GLYSYCTS",
        expr("round(GLYSYCTS*100/PMSKXSL,2) GLYSYCZB"),
        $"GLYSYCCS",
        $"GLYSYCSC",
        $"DYSXBPHTS",
        expr("round(DYSXBPHTS*100/PMSKXSL,2) DYSXBPHZB"),
        $"DYSXBPHCS",
        $"DYSXBPHSC",
        $"AGZTS",
        expr("round(AGZTS*100/PMSKXSL,2) AGZZB"),
        $"AGZCS",
        $"AGZSC",
        $"BGZTS",
        expr("round(BGZTS*100/PMSKXSL,2) BGZZB"),
        $"BGZCS",
        $"BGZSC",
        $"CGZTS",
        expr("round(CGZTS*100/PMSKXSL,2) CGZZB"),
        $"CGZCS",
        $"CGZSC",
        $"AZZTS",
        expr("round(AZZTS*100/PMSKXSL,2) AZZZB"),
        $"AZZCS",
        $"AZZSC",
        $"BZZTS",
        expr("round(BZZTS*100/PMSKXSL,2) BZZZB"),
        $"BZZCS",
        $"BZZSC",
        $"CZZTS",
        expr("round(CZZTS*100/PMSKXSL,2) CZZZB"),
        $"CZZCS",
        $"CZZSC",
        //        $"AQZTS",
        //        expr("round(AQZTS*100/PMSKXSL,2) AQZZB"),
        //        $"AQZCS",
        //        $"AQZSC",
        //        $"BQZTS",
        //        expr("round(BQZTS*100/PMSKXSL,2) BQZZB"),
        //        $"BQZCS",
        //        $"BQZSC",
        //        $"CQZTS",
        //        expr("round(CQZTS*100/PMSKXSL,2) CQZZB"),
        //        $"CQZCS",
        //        $"CQZSC",
        $"ZZTS",
        expr("round(ZZTS*100/PMSKXSL,2) ZZZB"),
        $"ZZCS",
        $"ZZSC",
        $"QZTS",
        expr("round(QZTS*100/PMSKXSL,2) QZZB"),
        $"QZCS",
        $"QZSC",
        $"DYYCTS",
        expr("round(DYYCTS*100/PMSKXSL,2) DYYCZB"),
        $"FZYCTS",
        expr("round(FZYCTS*100/PMSKXSL,2) FZYCZB"),
        $"FZL1TS",
        $"FZL2TS",
        $"FZL3TS",
        $"FZL4TS")//,
//        expr("null FZL5TS"),
//        expr("null FZL6TS"),
//        expr("null POWEROFF_PIONT_CS"))
    res
  }

  def statBZGatherSuccRateWeekAndMore: DataFrame = {
    import hc.implicits._
    val dateFrom = DateFormatUtil.dateFrom(statDate, statCycle)
    val dateTo = DateFormatUtil.dateTo(dateFrom, statCycle)

    val ycds = DateFormatUtil.statDays(dateFrom, dateTo, statCycle) * 96l

    val weekDateTo = DateFormatUtil.dateDay(dateTo, 1)
    val tjrq = if (DateFormatUtil.STAT_CYCLE_WEEK.equals(statCycle)) s"'${dateFrom}' TJKSRQ, '${weekDateTo}' TJJSRQ\n"
    else s"'${statDate}' TJRQ\n"

    val archSql = s"""select DWBM,
       DWMC,
       DWJB,
       CNW,
       SJ_DWBM,
       SJ_DWMC,
       PMSKXSL,
       XLXZ,
       YCFGKXSL,
       CJKXSL,
       ${tjrq}
  from pwyw_dwkxqxcjwzltj_bz_r
 where tjrq = (select max(tjrq) from pwyw_dwpbqxcjwzltj_ss_r)"""

    val arch = hc.read.jdbc(JdbcConnUtil.url, s"(${archSql})", JdbcConnUtil.connProp)

    val dataSql = s"""select DWBM,
       CNW,
       XLXZ,
       AXDYQXYCDS,
       AXDYQXSCDS,
       BXDYQXYCDS,
       BXDYQXSCDS,
       CXDYQXYCDS,
       CXDYQXSCDS,
       AXDLQXYCDS,
       AXDLQXSCDS,
       BXDLQXYCDS,
       BXDLQXSCDS,
       CXDLQXYCDS,
       CXDLQXSCDS,
       GLYSQXYCDS,
       GLYSQXSCDS,
       YGZGLQXYCDS,
       YGZGLQXSCDS,
       WGZGLQXYCDS,
       WGZGLQXSCDS
  from pwyw_dwkxqxcjwzltj_bz_r
 where tjrq >= to_date('${dateFrom}', 'yyyymmdd')
   and tjrq < to_date('${dateTo}', 'yyyymmdd')"""

    val data = hc.read.jdbc(JdbcConnUtil.url, s"(${dataSql})", JdbcConnUtil.connProp)

    val dfGroup = data.groupBy("DWBM", "XLXZ", "CNW")
    val df = dfGroup.agg(
      sum("AXDYQXYCDS") as "AXDYQXYCDS",
      sum("AXDYQXSCDS") as "AXDYQXSCDS",
      sum("BXDYQXYCDS") as "BXDYQXYCDS",
      sum("BXDYQXSCDS") as "BXDYQXSCDS",
      sum("CXDYQXYCDS") as "CXDYQXYCDS",
      sum("CXDYQXSCDS") as "CXDYQXSCDS",
      sum("AXDLQXYCDS") as "AXDLQXYCDS",
      sum("AXDLQXSCDS") as "AXDLQXSCDS",
      sum("BXDLQXYCDS") as "BXDLQXYCDS",
      sum("BXDLQXSCDS") as "BXDLQXSCDS",
      sum("CXDLQXYCDS") as "CXDLQXYCDS",
      sum("CXDLQXSCDS") as "CXDLQXSCDS",
      sum("GLYSQXYCDS") as "GLYSQXYCDS",
      sum("GLYSQXSCDS") as "GLYSQXSCDS",
      sum("YGZGLQXYCDS") as "YGZGLQXYCDS",
      sum("YGZGLQXSCDS") as "YGZGLQXSCDS",
      sum("WGZGLQXYCDS") as "WGZGLQXYCDS",
      sum("WGZGLQXSCDS") as "WGZGLQXSCDS")
    val res = arch.join(data, Seq("DWBM", "XLXZ", "CNW"))
    res
  }

  def statBZGatherSuccRate: DataFrame = {
    val df = if (DateFormatUtil.STAT_CYCLE_DAY.equals(statCycle)) {
      statBZGatherSuccRateDay
    } else {
      statBZGatherSuccRateWeekAndMore
    }
    df
  }

  def statRunStatusDay: DataFrame = {
    import hc.implicits._
    val dateFrom = DateFormatUtil.dateFrom(statDate, DateFormatUtil.STAT_CYCLE_DAY)
    val dateTo = DateFormatUtil.dateTo(dateFrom, DateFormatUtil.STAT_CYCLE_DAY)
    val ycds = DateFormatUtil.statDays(dateFrom, dateTo, DateFormatUtil.STAT_CYCLE_DAY) * 96l
    val tjrq = s"'${statDate}' TJRQ\n"

    val sql1 = s"""select a.obj_id XLID,
                             a.XLMC,
                             a.YXBH,
                             a.DYDJ,
                             a.SSDS,
                             a.YWDW,
                             a.YWDWMC,
                             d.pms_dwbm DWBM,
                             d.pms_dwmc DWMC,
                             d.PMS_DWCJ DWJB,
                             d.sjdwid SJDWBM,
                             d.SJDWMC,
                             a.whbz BZID,
                             a.qddz SSDZ,
                             a.sfnw CNW,
                             a.ZXID,
                             a.SSZX,
                             a.XLXZ,
                             a.ZYCD,
                             a.QDDZ,
                             a.ZDDZ,
                             a.TYRQ,
                             a.JSFS,
                             a.ZDYXDL,
                             a.JJDL,
                             a.YXFHXE,
                             a.EDSSGL,
                             a.XLZCD,
                             a.JKXLCD,
                             a.DLXLCD,
                             a.JKJXFS,
                             ${tjrq}
                        from pwyw_arch.T_SB_ZWYC_xl a
                        join pwyw_arch.ST_PMS_YX_DW d
                          on (a.WHBZ = d.PMS_DWID)"""

    val sql2 =
      "select xlid ID, sjsj DATA_TIME, UAB UA, UBC UB, UCA UC, IA, IB, IC, P, Q, null F,sqrt(P*P+Q*Q) S, 1 T_FACTOR from pwyw_arch.E_DMS_XL_CURVE\n" +
        s" where dt >= '${dateFrom}'\n" +
        s"   and dt < '${dateTo}'"

    val t1 = hc.sql(sql1).distinct()
    val t2 = hc.sql(sql2)

    val t2Group = t2.as[DataBean]
      .groupBy($"ID")
      .mapGroups((key, row) => {
        val seq = row.toSeq
        val maxUA = if (seq.filter { c => c.UA.isDefined }.isEmpty) null else seq.filter { c => c.UA.isDefined }.maxBy { c => c.UA }
        val maxUB = if (seq.filter { c => c.UB.isDefined }.isEmpty) null else seq.filter { c => c.UB.isDefined }.maxBy { c => c.UB }
        val maxUC = if (seq.filter { c => c.UC.isDefined }.isEmpty) null else seq.filter { c => c.UC.isDefined }.maxBy { c => c.UC }
        val maxIA = if (seq.filter { c => c.IA.isDefined }.isEmpty) null else seq.filter { c => c.IA.isDefined }.maxBy { c => c.IA }
        val maxIB = if (seq.filter { c => c.IB.isDefined }.isEmpty) null else seq.filter { c => c.IB.isDefined }.maxBy { c => c.IB }
        val maxIC = if (seq.filter { c => c.IC.isDefined }.isEmpty) null else seq.filter { c => c.IC.isDefined }.maxBy { c => c.IC }
        val maxP = if (seq.filter { c => c.P.isDefined }.isEmpty) null else seq.filter { c => c.P.isDefined }.maxBy { c => c.P }
        val maxQ = if (seq.filter { c => c.Q.isDefined }.isEmpty) null else seq.filter { c => c.Q.isDefined }.maxBy { c => c.Q }
        val maxS = if (seq.filter { c => c.S.isDefined }.isEmpty) null else seq.filter { c => c.S.isDefined }.maxBy { c => c.S }

        val minUA = if (seq.filter { c => c.UA.isDefined }.isEmpty) null else seq.filter { c => c.UA.isDefined }.minBy { c => c.UA }
        val minUB = if (seq.filter { c => c.UB.isDefined }.isEmpty) null else seq.filter { c => c.UB.isDefined }.minBy { c => c.UB }
        val minUC = if (seq.filter { c => c.UC.isDefined }.isEmpty) null else seq.filter { c => c.UC.isDefined }.minBy { c => c.UC }
        val minIA = if (seq.filter { c => c.IA.isDefined }.isEmpty) null else seq.filter { c => c.IA.isDefined }.minBy { c => c.IA }
        val minIB = if (seq.filter { c => c.IB.isDefined }.isEmpty) null else seq.filter { c => c.IB.isDefined }.minBy { c => c.IB }
        val minIC = if (seq.filter { c => c.IC.isDefined }.isEmpty) null else seq.filter { c => c.IC.isDefined }.minBy { c => c.IC }
        val minP = if (seq.filter { c => c.P.isDefined }.isEmpty) null else seq.filter { c => c.P.isDefined }.minBy { c => c.P }
        val minQ = if (seq.filter { c => c.Q.isDefined }.isEmpty) null else seq.filter { c => c.Q.isDefined }.minBy { c => c.Q }
        val minF = if (seq.filter { c => c.F.isDefined }.isEmpty) null else seq.filter { c => c.F.isDefined }.minBy { c => c.F }
        val minS = if (seq.filter { c => c.S.isDefined }.isEmpty) null else seq.filter { c => c.S.isDefined }.minBy { c => c.S }

        val countUA = seq.count { c => c.UA.isDefined }
        val countUB = seq.count { c => c.UB.isDefined }
        val countUC = seq.count { c => c.UC.isDefined }
        val countIA = seq.count { c => c.IA.isDefined }
        val countIB = seq.count { c => c.IB.isDefined }
        val countIC = seq.count { c => c.IC.isDefined }
        val countP = seq.count { c => c.P.isDefined }
        val countQ = seq.count { c => c.Q.isDefined }
        val countF = seq.count { c => c.F.isDefined }

        val sumBean = seq.reduce((a, b) => {
          new DataBean(a.ID,
            a.DATA_TIME,
            a.UA,
            a.UB,
            a.UC,
            a.IA,
            a.IB,
            a.IC,
            if (a.P.isDefined && b.P.isDefined) Some(a.P.get + b.P.get) else None,
            a.Q,
            a.F,
            a.S,
            a.T_FACTOR)
        })

        val avgP: java.lang.Double = if (sumBean.P.isDefined) sumBean.P.get / countP else null
        val fhl: java.lang.Double = if (maxP != null && maxP.P.isDefined && avgP != null) avgP / maxP.P.get else null
        val MAXS_T: java.lang.Double = if (maxS != null && maxS.S.isDefined && maxS.T_FACTOR.isDefined) maxS.S.get * maxS.T_FACTOR.get else null

        new RunStatusBean(Some(key.getAs[String](0)),
          if (maxP != null) maxP.P else None,
          if (maxP != null) maxP.DATA_TIME else None,
          if (minP != null) minP.P else None,
          if (minP != null) minP.DATA_TIME else None,
          if (maxS != null) Some(Math.sqrt(maxS.Q.get * maxS.Q.get + maxS.P.get * maxS.P.get)) else None,
          if (maxS != null) maxS.DATA_TIME else None,
          if (minF != null) minF.F else None,
          if (minF != null) minF.DATA_TIME else None,
          if (maxIA != null) maxIA.IA else None,
          if (maxIA != null) maxIA.DATA_TIME else None,
          if (minIA != null) minIA.IA else None,
          if (minIA != null) minIA.DATA_TIME else None,
          if (maxIB != null) maxIB.IB else None,
          if (maxIB != null) maxIB.DATA_TIME else None,
          if (minIB != null) minIB.IB else None,
          if (minIB != null) minIB.DATA_TIME else None,
          if (maxIC != null) maxIC.IC else None,
          if (maxIC != null) maxIC.DATA_TIME else None,
          if (minIC != null) minIC.IC else None,
          if (minIC != null) minIC.DATA_TIME else None,
          if (minUA != null) minUA.UA else None,
          if (minUA != null) minUA.DATA_TIME else None,
          if (minUB != null) minUB.UB else None,
          if (minUB != null) minUB.DATA_TIME else None,
          if (minUC != null) minUC.UC else None,
          if (minUC != null) minUC.DATA_TIME else None,
          if (maxUA != null) maxUA.UA else None,
          if (maxUA != null) maxUA.DATA_TIME else None,
          if (maxUB != null) maxUB.UB else None,
          if (maxUB != null) maxUB.DATA_TIME else None,
          if (maxUC != null) maxUC.UC else None,
          if (maxUC != null) maxUC.DATA_TIME else None,
          if (avgP != null) Some(avgP) else None,
          if (fhl != null) Some(fhl) else None,
          Some(ycds.toString.toInt),
          Some(countUA),
          Some(ycds.toString.toInt),
          Some(countUB),
          Some(ycds.toString.toInt),
          Some(countUC),
          Some(ycds.toString.toInt),
          Some(countIA),
          Some(ycds.toString.toInt),
          Some(countIB),
          Some(ycds.toString.toInt),
          Some(countIC),
          Some(ycds.toString.toInt),
          Some(countF),
          Some(ycds.toString.toInt),
          Some(countP),
          Some(ycds.toString.toInt),
          Some(countQ),
          if (MAXS_T != null) Some(MAXS_T) else None)
      }).toDF().withColumnRenamed("ID", "XLID")

    val joinData = t1.join(t2Group, Seq("XLID"), "left_outer")
    val res = joinData.select($"XLID",
      $"XLMC",
      $"YXBH",
      $"DYDJ",
      $"SSDS",
      $"YWDW",
      $"YWDWMC",
      $"DWBM",
      $"DWMC",
      $"DWJB",
      $"SJDWBM",
      $"SJDWMC",
      $"BZID",
      $"SSDZ",
      $"CNW",
      $"ZXID",
      $"SSZX",
      $"XLXZ",
      $"ZYCD",
      $"QDDZ",
      $"ZDDZ",
      $"TYRQ",
      $"JSFS",
      $"ZDYXDL",
      $"JJDL",
      $"YXFHXE",
      $"EDSSGL",
      $"XLZCD",
      $"JKXLCD",
      $"DLXLCD",
      $"JKJXFS",
      $"TJRQ",
      $"ZDYGGL",
      $"ZDYGGL_SJ",
      $"ZXYGGL",
      $"ZXYGGL_SJ",
      $"ZDSZGL",
      $"ZDSZGL_SJ",
      $"ZXGLYS",
      $"ZXGLYS_SJ",
      $"ZDDL_A",
      $"ZDDL_A_SJ",
      $"ZXDL_A",
      $"ZXDL_A_SJ",
      $"ZDDL_B",
      $"ZDDL_B_SJ",
      $"ZXDL_B",
      $"ZXDL_B_SJ",
      $"ZDDL_C",
      $"ZDDL_C_SJ",
      $"ZXDL_C",
      $"ZXDL_C_SJ",
      $"ZXDY_A",
      $"ZXDY_A_SJ",
      $"ZXDY_B",
      $"ZXDY_B_SJ",
      $"ZXDY_C",
      $"ZXDY_C_SJ",
      $"ZDDY_A",
      $"ZDDY_A_SJ",
      $"ZDDY_B",
      $"ZDDY_B_SJ",
      $"ZDDY_C",
      $"ZDDY_C_SJ",
      $"PJFH",
      $"FHL",
      expr("MAXS_T*100/EDSSGL FZL"),
      $"AXDYQXYCDS",
      $"AXDYQXSCDS",
      $"BXDYQXYCDS",
      $"BXDYQXSCDS",
      $"CXDYQXYCDS",
      $"CXDYQXSCDS",
      $"AXDLQXYCDS",
      $"AXDLQXSCDS",
      $"BXDLQXYCDS",
      $"BXDLQXSCDS",
      $"CXDLQXYCDS",
      $"CXDLQXSCDS",
      $"GLYSQXYCDS",
      $"GLYSQXSCDS",
      $"YGZGLQXYCDS",
      $"YGZGLQXSCDS",
      $"WGZGLQXYCDS",
      $"WGZGLQXSCDS")//,
//      expr("null ZXYGZDNL"),
//      expr("null ZXYGZDNL1"),
//      expr("null ZXYGZDNL2"),
//      expr("null ZXYGZDNL3"),
//      expr("null ZXYGZDNL4"),
//      expr("null FXYGZDNL"),
//      expr("null FXYGZDNL1"),
//      expr("null FXYGZDNL2"),
//      expr("null FXYGZDNL3"),
//      expr("null FXYGZDNL4"),
//      expr("null ZXWGZDNL"),
//      expr("null FXWGZDNL"),
//      expr("null XX1_R"),
//      expr("null XX4_R"),
//
//      expr("null AVG_F"),
//      expr("null FDL"),
//      expr("null GDL"),
//      expr("null SDL"),
//      expr("null XSL"),
//      expr("null POWEROFF_PIONT_CS"))
    res
  }

  def statRunStatusWeekAndMon: DataFrame = {
    import hc.implicits._
    val dateFrom = DateFormatUtil.dateFrom(statDate, statCycle)
    val dateTo = DateFormatUtil.dateTo(dateFrom, statCycle)
    val ycds = DateFormatUtil.statDays(dateFrom, dateTo, statCycle) * 96l
    val weekDateTo = DateFormatUtil.dateDay(dateTo, 1)
    val tjrq = if (DateFormatUtil.STAT_CYCLE_WEEK.equals(statCycle)) s"'${dateFrom}' TJKSRQ, '${weekDateTo}' TJJSRQ\n"
    else s"'${statDate}' TJRQ\n"

    val sql1 = s"""select XLID,
       XLMC,
       YXBH,
       DYDJ,
       SSDS,
       YWDW,
       YWDWMC,
       DWBM,
       DWMC,
       DWJB,
       SJDWBM,
       SJDWMC,
       BZID,
       SSDZ,
       CNW,
       ZXID,
       SSZX,
       XLXZ,
       ZYCD,
       QDDZ,
       ZDDZ,
       TYRQ,
       JSFS,
       ZDYXDL,
       JJDL,
       YXFHXE,
       EDSSGL,
       XLZCD,
       JKXLCD,
       DLXLCD,
       JKJXFS,
       ${tjrq}
  from pwyw_kxyxzttj_r
 where tjrq >= to_date('${dateFrom}', 'yyyymmdd')
   and tjrq < to_date('${dateTo}', 'yyyymmdd')
 group by XLID,
       XLMC,
       YXBH,
       DYDJ,
       SSDS,
       YWDW,
       YWDWMC,
       DWBM,
       DWMC,
       DWJB,
       SJDWBM,
       SJDWMC,
       BZID,
       SSDZ,
       CNW,
       ZXID,
       SSZX,
       XLXZ,
       ZYCD,
       QDDZ,
       ZDDZ,
       TYRQ,
       JSFS,
       ZDYXDL,
       JJDL,
       YXFHXE,
       EDSSGL,
       XLZCD,
       JKXLCD,
       DLXLCD,
       JKJXFS"""
    val t1 = hc.read.jdbc(JdbcConnUtil.url, s"(${sql1})", JdbcConnUtil.connProp)

    val sql2 = s"""select XLID as ID,
       ZDYGGL,
       to_char(ZDYGGL_SJ,'yyyy-mm-dd hh24:mi:ss')||'.0' ZDYGGL_SJ,
       ZXYGGL,
       to_char(ZXYGGL_SJ,'yyyy-mm-dd hh24:mi:ss')||'.0' ZXYGGL_SJ,
       ZDSZGL,
       to_char(ZDSZGL_SJ,'yyyy-mm-dd hh24:mi:ss')||'.0' ZDSZGL_SJ,
       ZXGLYS,
       to_char(ZXGLYS_SJ,'yyyy-mm-dd hh24:mi:ss')||'.0' ZXGLYS_SJ,
       ZDDL_A,
       to_char(ZDDL_A_SJ,'yyyy-mm-dd hh24:mi:ss')||'.0' ZDDL_A_SJ,
       ZXDL_A,
       to_char(ZXDL_A_SJ,'yyyy-mm-dd hh24:mi:ss')||'.0' ZXDL_A_SJ,
       ZDDL_B,
       to_char(ZDDL_B_SJ,'yyyy-mm-dd hh24:mi:ss')||'.0' ZDDL_B_SJ,
       ZXDL_B,
       to_char(ZXDL_B_SJ,'yyyy-mm-dd hh24:mi:ss')||'.0' ZXDL_B_SJ,
       ZDDL_C,
       to_char(ZDDL_C_SJ,'yyyy-mm-dd hh24:mi:ss')||'.0' ZDDL_C_SJ,
       ZXDL_C,
       to_char(ZXDL_C_SJ,'yyyy-mm-dd hh24:mi:ss')||'.0' ZXDL_C_SJ,
       ZXDY_A,
       to_char(ZXDL_C_SJ,'yyyy-mm-dd hh24:mi:ss')||'.0' ZXDY_A_SJ,
       ZXDY_B,
       to_char(ZXDY_B_SJ,'yyyy-mm-dd hh24:mi:ss')||'.0' ZXDY_B_SJ,
       ZXDY_C,
       to_char(ZXDY_C_SJ,'yyyy-mm-dd hh24:mi:ss')||'.0' ZXDY_C_SJ,
       ZDDY_A,
       to_char(ZDDY_A_SJ,'yyyy-mm-dd hh24:mi:ss')||'.0' ZDDY_A_SJ,
       ZDDY_B,
       to_char(ZDDY_B_SJ,'yyyy-mm-dd hh24:mi:ss')||'.0' ZDDY_B_SJ,
       ZDDY_C,
       to_char(ZDDY_C_SJ,'yyyy-mm-dd hh24:mi:ss')||'.0' ZDDY_C_SJ,
       PJGL PJFH,
       FHL,
       FZL,
       AXDYQXYCDS,
       AXDYQXSCDS,
       BXDYQXYCDS,
       BXDYQXSCDS,
       CXDYQXYCDS,
       CXDYQXSCDS,
       AXDLQXYCDS,
       AXDLQXSCDS,
       BXDLQXYCDS,
       BXDLQXSCDS,
       CXDLQXYCDS,
       CXDLQXSCDS,
       GLYSQXYCDS,
       GLYSQXSCDS,
       YGZGLQXYCDS,
       YGZGLQXSCDS,
       WGZGLQXYCDS,
       WGZGLQXSCDS
  from pwyw_kxyxzttj_r
 where tjrq >= to_date('${dateFrom}', 'yyyymmdd')
   and tjrq < to_date('${dateTo}', 'yyyymmdd')"""

    val t2 = hc.read.jdbc(JdbcConnUtil.url, s"(${sql2})", JdbcConnUtil.connProp)
      .select($"ID",
        expr("cast(ZDYGGL as double)") as "ZDYGGL",
        $"ZDYGGL_SJ",
        expr("cast(ZXYGGL as double)") as "ZXYGGL",
        $"ZXYGGL_SJ",
        expr("cast(ZDSZGL as double)") as "ZDSZGL",
        $"ZDSZGL_SJ",
        expr("cast(ZXGLYS as double)") as "ZXGLYS",
        $"ZXGLYS_SJ",
        expr("cast(ZDDL_A as double)") as "ZDDL_A",
        $"ZDDL_A_SJ",
        expr("cast(ZXDL_A as double)") as "ZXDL_A",
        $"ZXDL_A_SJ",
        expr("cast(ZDDL_B as double)") as "ZDDL_B",
        $"ZDDL_B_SJ",
        expr("cast(ZXDL_B as double)") as "ZXDL_B",
        $"ZXDL_B_SJ",
        expr("cast(ZDDL_C as double)") as "ZDDL_C",
        $"ZDDL_C_SJ",
        expr("cast(ZXDL_C as double)") as "ZXDL_C",
        $"ZXDL_C_SJ",
        expr("cast(ZXDY_A as double)") as "ZXDY_A",
        $"ZXDY_A_SJ",
        expr("cast(ZXDY_B as double)") as "ZXDY_B",
        $"ZXDY_B_SJ",
        expr("cast(ZXDY_C as double)") as "ZXDY_C",
        $"ZXDY_C_SJ",
        expr("cast(ZDDY_A as double)") as "ZDDY_A",
        $"ZDDY_A_SJ",
        expr("cast(ZDDY_B as double)") as "ZDDY_B",
        $"ZDDY_B_SJ",
        expr("cast(ZDDY_C as double)") as "ZDDY_C",
        $"ZDDY_C_SJ",
        expr("cast(PJFH as double)") as "PJFH",
        expr("cast(FHL as double)") as "FHL",
        expr("cast(FZL as double)") as "FZL",
        expr("cast(AXDYQXYCDS as int)") as "AXDYQXYCDS",
        expr("cast(AXDYQXSCDS as int)") as "AXDYQXSCDS",
        expr("cast(BXDYQXYCDS as int)") as "BXDYQXYCDS",
        expr("cast(BXDYQXSCDS as int)") as "BXDYQXSCDS",
        expr("cast(CXDYQXYCDS as int)") as "CXDYQXYCDS",
        expr("cast(CXDYQXSCDS as int)") as "CXDYQXSCDS",
        expr("cast(AXDLQXYCDS as int)") as "AXDLQXYCDS",
        expr("cast(AXDLQXSCDS as int)") as "AXDLQXSCDS",
        expr("cast(BXDLQXYCDS as int)") as "BXDLQXYCDS",
        expr("cast(BXDLQXSCDS as int)") as "BXDLQXSCDS",
        expr("cast(CXDLQXYCDS as int)") as "CXDLQXYCDS",
        expr("cast(CXDLQXSCDS as int)") as "CXDLQXSCDS",
        expr("cast(GLYSQXYCDS as int)") as "GLYSQXYCDS",
        expr("cast(GLYSQXSCDS as int)") as "GLYSQXSCDS",
        expr("cast(YGZGLQXYCDS as int)") as "YGZGLQXYCDS",
        expr("cast(YGZGLQXSCDS as int)") as "YGZGLQXSCDS",
        expr("cast(WGZGLQXYCDS as int)") as "WGZGLQXYCDS",
        expr("cast(WGZGLQXSCDS as int)") as "WGZGLQXSCDS")

    val t2Group = t2.as[OracleRunStatusBean]
      .groupBy($"ID")
      .mapGroups((key, row) => {
        val seq = row.toSeq
        val ZDDY_A = if (seq.filter { c => c.ZDDY_A.isDefined }.isEmpty) null else seq.filter { c => c.ZDDY_A.isDefined }.maxBy { c => c.ZDDY_A }
        val ZDDY_B = if (seq.filter { c => c.ZDDY_B.isDefined }.isEmpty) null else seq.filter { c => c.ZDDY_B.isDefined }.maxBy { c => c.ZDDY_B }
        val ZDDY_C = if (seq.filter { c => c.ZDDY_C.isDefined }.isEmpty) null else seq.filter { c => c.ZDDY_C.isDefined }.maxBy { c => c.ZDDY_C }
        val ZDDL_A = if (seq.filter { c => c.ZDDL_A.isDefined }.isEmpty) null else seq.filter { c => c.ZDDL_A.isDefined }.maxBy { c => c.ZDDL_A }
        val ZDDL_B = if (seq.filter { c => c.ZDDL_B.isDefined }.isEmpty) null else seq.filter { c => c.ZDDL_B.isDefined }.maxBy { c => c.ZDDL_B }
        val ZDDL_C = if (seq.filter { c => c.ZDDL_C.isDefined }.isEmpty) null else seq.filter { c => c.ZDDL_C.isDefined }.maxBy { c => c.ZDDL_C }
        val ZDYGGL = if (seq.filter { c => c.ZDYGGL.isDefined }.isEmpty) null else seq.filter { c => c.ZDYGGL.isDefined }.maxBy { c => c.ZDYGGL }
        val ZDSZGL = if (seq.filter { c => c.ZDSZGL.isDefined }.isEmpty) null else seq.filter { c => c.ZDSZGL.isDefined }.maxBy { c => c.ZDSZGL }

        val ZXDY_A = if (seq.filter { c => c.ZXDY_A.isDefined }.isEmpty) null else seq.filter { c => c.ZXDY_A.isDefined }.minBy { c => c.ZXDY_A }
        val ZXDY_B = if (seq.filter { c => c.ZXDY_B.isDefined }.isEmpty) null else seq.filter { c => c.ZXDY_B.isDefined }.minBy { c => c.ZXDY_B }
        val ZXDY_C = if (seq.filter { c => c.ZXDY_C.isDefined }.isEmpty) null else seq.filter { c => c.ZXDY_C.isDefined }.minBy { c => c.ZXDY_C }
        val ZXDL_A = if (seq.filter { c => c.ZXDL_A.isDefined }.isEmpty) null else seq.filter { c => c.ZXDL_A.isDefined }.minBy { c => c.ZXDL_A }
        val ZXDL_B = if (seq.filter { c => c.ZXDL_B.isDefined }.isEmpty) null else seq.filter { c => c.ZXDL_B.isDefined }.minBy { c => c.ZXDL_B }
        val ZXDL_C = if (seq.filter { c => c.ZXDL_C.isDefined }.isEmpty) null else seq.filter { c => c.ZXDL_C.isDefined }.minBy { c => c.ZXDL_C }
        val ZXYGGL = if (seq.filter { c => c.ZXYGGL.isDefined }.isEmpty) null else seq.filter { c => c.ZXYGGL.isDefined }.minBy { c => c.ZXYGGL }

        val FZL = if (seq.filter { c => c.FZL.isDefined }.isEmpty) null else seq.filter { c => c.FZL.isDefined }.maxBy { c => c.FZL }

        val count = seq.count { c => c.ID.isDefined }

        val sumBean = seq.reduce((a, b) => {
          new OracleRunStatusBean(a.ID,
            a.ZDYGGL,
            a.ZDYGGL_SJ,
            a.ZXYGGL,
            a.ZXYGGL_SJ,
            a.ZDSZGL,
            a.ZDSZGL_SJ,
            a.ZXGLYS,
            a.ZXGLYS_SJ,
            a.ZDDL_A,
            a.ZDDL_A_SJ,
            a.ZXDL_A,
            a.ZXDL_A_SJ,
            a.ZDDL_B,
            a.ZDDL_B_SJ,
            a.ZXDL_B,
            a.ZXDL_B_SJ,
            a.ZDDL_C,
            a.ZDDL_C_SJ,
            a.ZXDL_C,
            a.ZXDL_C_SJ,
            a.ZXDY_A,
            a.ZXDY_A_SJ,
            a.ZXDY_B,
            a.ZXDY_B_SJ,
            a.ZXDY_C,
            a.ZXDY_C_SJ,
            a.ZDDY_A,
            a.ZDDY_A_SJ,
            a.ZDDY_B,
            a.ZDDY_B_SJ,
            a.ZDDY_C,
            a.ZDDY_C_SJ,
            if (a.PJFH.isDefined && b.PJFH.isDefined) Some(a.PJFH.get + b.PJFH.get) else None,
            a.FHL,
            if (a.AXDYQXYCDS.isDefined && b.AXDYQXYCDS.isDefined) Some(a.AXDYQXYCDS.get + b.AXDYQXYCDS.get) else None,
            if (a.AXDYQXSCDS.isDefined && b.AXDYQXSCDS.isDefined) Some(a.AXDYQXSCDS.get + b.AXDYQXSCDS.get) else None,
            if (a.BXDYQXYCDS.isDefined && b.BXDYQXYCDS.isDefined) Some(a.BXDYQXYCDS.get + b.BXDYQXYCDS.get) else None,
            if (a.BXDYQXSCDS.isDefined && b.BXDYQXSCDS.isDefined) Some(a.BXDYQXSCDS.get + b.BXDYQXSCDS.get) else None,
            if (a.CXDYQXYCDS.isDefined && b.CXDYQXYCDS.isDefined) Some(a.CXDYQXYCDS.get + b.CXDYQXYCDS.get) else None,
            if (a.CXDYQXSCDS.isDefined && b.CXDYQXSCDS.isDefined) Some(a.CXDYQXSCDS.get + b.CXDYQXSCDS.get) else None,
            if (a.AXDLQXYCDS.isDefined && b.AXDLQXYCDS.isDefined) Some(a.AXDLQXYCDS.get + b.AXDLQXYCDS.get) else None,
            if (a.AXDLQXSCDS.isDefined && b.AXDLQXSCDS.isDefined) Some(a.AXDLQXSCDS.get + b.AXDLQXSCDS.get) else None,
            if (a.BXDLQXYCDS.isDefined && b.BXDLQXYCDS.isDefined) Some(a.BXDLQXYCDS.get + b.BXDLQXYCDS.get) else None,
            if (a.BXDLQXSCDS.isDefined && b.BXDLQXSCDS.isDefined) Some(a.BXDLQXSCDS.get + b.BXDLQXSCDS.get) else None,
            if (a.CXDLQXYCDS.isDefined && b.CXDLQXYCDS.isDefined) Some(a.CXDLQXYCDS.get + b.CXDLQXYCDS.get) else None,
            if (a.CXDLQXSCDS.isDefined && b.CXDLQXSCDS.isDefined) Some(a.CXDLQXSCDS.get + b.CXDLQXSCDS.get) else None,
            if (a.GLYSQXYCDS.isDefined && b.GLYSQXYCDS.isDefined) Some(a.GLYSQXYCDS.get + b.GLYSQXYCDS.get) else None,
            if (a.GLYSQXSCDS.isDefined && b.GLYSQXSCDS.isDefined) Some(a.GLYSQXSCDS.get + b.GLYSQXSCDS.get) else None,
            if (a.YGZGLQXYCDS.isDefined && b.YGZGLQXYCDS.isDefined) Some(a.YGZGLQXYCDS.get + b.YGZGLQXYCDS.get) else None,
            if (a.YGZGLQXSCDS.isDefined && b.YGZGLQXSCDS.isDefined) Some(a.YGZGLQXSCDS.get + b.YGZGLQXSCDS.get) else None,
            if (a.WGZGLQXYCDS.isDefined && b.WGZGLQXYCDS.isDefined) Some(a.WGZGLQXYCDS.get + b.WGZGLQXYCDS.get) else None,
            if (a.WGZGLQXSCDS.isDefined && b.WGZGLQXSCDS.isDefined) Some(a.WGZGLQXSCDS.get + b.WGZGLQXSCDS.get) else None,
            None)
        })

        val pjfh: java.lang.Double = if (sumBean.PJFH.isDefined) sumBean.PJFH.get / count else null

        val fhl: java.lang.Double = if (ZDYGGL != null && ZDYGGL.ZDYGGL.isDefined && pjfh != null) pjfh / ZDYGGL.ZDYGGL.get else null

        new OracleRunStatusBean(Some(key.getAs[String](0)),
          if (ZDYGGL != null) ZDYGGL.ZDYGGL else None,
          if (ZDYGGL != null) ZDYGGL.ZDYGGL_SJ else None,
          if (ZXYGGL != null) ZXYGGL.ZXYGGL else None,
          if (ZXYGGL != null) ZXYGGL.ZXYGGL_SJ else None,
          if (ZDSZGL != null) ZDSZGL.ZDSZGL else None,
          if (ZDSZGL != null) ZDSZGL.ZDSZGL_SJ else None,
          None,
          None,
          //          if (minF != null) minF.F else None,
          //          if (minF != null) minF.DATA_TIME else None,
          if (ZDDL_A != null) ZDDL_A.ZDDL_A else None,
          if (ZDDL_A != null) ZDDL_A.ZDDL_A_SJ else None,
          if (ZXDL_A != null) ZXDL_A.ZXDL_A else None,
          if (ZXDL_A != null) ZXDL_A.ZXDL_A_SJ else None,
          if (ZDDL_B != null) ZDDL_B.ZDDL_B else None,
          if (ZDDL_B != null) ZDDL_B.ZDDL_B_SJ else None,
          if (ZXDL_B != null) ZXDL_B.ZXDL_B else None,
          if (ZXDL_B != null) ZXDL_B.ZXDL_B_SJ else None,
          if (ZDDL_C != null) ZDDL_C.ZDDL_C else None,
          if (ZDDL_C != null) ZDDL_C.ZDDL_C_SJ else None,
          if (ZXDL_C != null) ZXDL_C.ZXDL_C else None,
          if (ZXDL_C != null) ZXDL_C.ZXDL_C_SJ else None,
          if (ZXDY_A != null) ZXDY_A.ZXDY_A else None,
          if (ZXDY_A != null) ZXDY_A.ZXDY_A_SJ else None,
          if (ZXDY_B != null) ZXDY_B.ZXDY_B else None,
          if (ZXDY_B != null) ZXDY_B.ZXDY_B_SJ else None,
          if (ZXDY_C != null) ZXDY_C.ZXDY_C else None,
          if (ZXDY_C != null) ZXDY_C.ZXDY_C_SJ else None,
          if (ZDDY_A != null) ZDDY_A.ZDDY_A else None,
          if (ZDDY_A != null) ZDDY_A.ZDDY_A_SJ else None,
          if (ZDDY_B != null) ZDDY_B.ZDDY_B else None,
          if (ZDDY_B != null) ZDDY_B.ZDDY_B_SJ else None,
          if (ZDDY_C != null) ZDDY_C.ZDDY_C else None,
          if (ZDDY_C != null) ZDDY_C.ZDDY_C_SJ else None,
          if (pjfh != null) Some(pjfh) else None,
          if (fhl != null) Some(fhl) else None,
          sumBean.AXDYQXYCDS,
          sumBean.AXDYQXSCDS,
          sumBean.BXDYQXYCDS,
          sumBean.BXDYQXSCDS,
          sumBean.CXDYQXYCDS,
          sumBean.CXDYQXSCDS,
          sumBean.AXDLQXYCDS,
          sumBean.AXDLQXSCDS,
          sumBean.BXDLQXYCDS,
          sumBean.BXDLQXSCDS,
          sumBean.CXDLQXYCDS,
          sumBean.CXDLQXSCDS,
          sumBean.GLYSQXYCDS,
          sumBean.GLYSQXSCDS,
          sumBean.YGZGLQXYCDS,
          sumBean.YGZGLQXSCDS,
          sumBean.WGZGLQXYCDS,
          sumBean.WGZGLQXSCDS,
          if (FZL != null) FZL.FZL else None)
      }).toDF().withColumnRenamed("ID", "XLID")

    val joinData = t1.join(t2Group, Seq("XLID"), "left_outer")

    val res = if (DateFormatUtil.STAT_CYCLE_WEEK.equals(statCycle))
      joinData.select($"XLID",
        $"XLMC",
        $"YXBH",
        $"DYDJ",
        $"SSDS",
        $"YWDW",
        $"YWDWMC",
        $"DWBM",
        $"DWMC",
        $"DWJB",
        $"SJDWBM",
        $"SJDWMC",
        $"BZID",
        $"SSDZ",
        $"CNW",
        $"ZXID",
        $"SSZX",
        $"XLXZ",
        $"ZYCD",
        $"QDDZ",
        $"ZDDZ",
        $"TYRQ",
        $"JSFS",
        $"ZDYXDL",
        $"JJDL",
        $"YXFHXE",
        $"EDSSGL",
        $"XLZCD",
        $"JKXLCD",
        $"DLXLCD",
        $"JKJXFS",
        $"TJKSRQ",
        $"TJJSRQ",
        $"ZDYGGL",
        $"ZDYGGL_SJ",
        $"ZXYGGL",
        $"ZXYGGL_SJ",
        $"ZDSZGL",
        $"ZDSZGL_SJ",
        $"ZXGLYS",
        $"ZXGLYS_SJ",
        $"ZDDL_A",
        $"ZDDL_A_SJ",
        $"ZXDL_A",
        $"ZXDL_A_SJ",
        $"ZDDL_B",
        $"ZDDL_B_SJ",
        $"ZXDL_B",
        $"ZXDL_B_SJ",
        $"ZDDL_C",
        $"ZDDL_C_SJ",
        $"ZXDL_C",
        $"ZXDL_C_SJ",
        $"ZXDY_A",
        $"ZXDY_A_SJ",
        $"ZXDY_B",
        $"ZXDY_B_SJ",
        $"ZXDY_C",
        $"ZXDY_C_SJ",
        $"ZDDY_A",
        $"ZDDY_A_SJ",
        $"ZDDY_B",
        $"ZDDY_B_SJ",
        $"ZDDY_C",
        $"ZDDY_C_SJ",
        $"PJFH",
        $"FHL",
        $"FZL",
        $"AXDYQXYCDS",
        $"AXDYQXSCDS",
        $"BXDYQXYCDS",
        $"BXDYQXSCDS",
        $"CXDYQXYCDS",
        $"CXDYQXSCDS",
        $"AXDLQXYCDS",
        $"AXDLQXSCDS",
        $"BXDLQXYCDS",
        $"BXDLQXSCDS",
        $"CXDLQXYCDS",
        $"CXDLQXSCDS",
        $"GLYSQXYCDS",
        $"GLYSQXSCDS",
        $"YGZGLQXYCDS",
        $"YGZGLQXSCDS",
        $"WGZGLQXYCDS",
        $"WGZGLQXSCDS")//,
//        expr("null ZXYGZDNL"),
//        expr("null ZXYGZDNL1"),
//        expr("null ZXYGZDNL2"),
//        expr("null ZXYGZDNL3"),
//        expr("null ZXYGZDNL4"),
//        expr("null FXYGZDNL"),
//        expr("null FXYGZDNL1"),
//        expr("null FXYGZDNL2"),
//        expr("null FXYGZDNL3"),
//        expr("null FXYGZDNL4"),
//        expr("null ZXWGZDNL"),
//        expr("null FXWGZDNL"),
//        expr("null XX1_R"),
//        expr("null XX4_R"),
//
//        expr("null AVG_F"),
//        expr("null FDL"),
//        expr("null GDL"),
//        expr("null SDL"),
//        expr("null XSL"),
//        expr("null POWEROFF_PIONT_CS"))
    else
      joinData.select($"XLID",
        $"XLMC",
        $"YXBH",
        $"DYDJ",
        $"SSDS",
        $"YWDW",
        $"YWDWMC",
        $"DWBM",
        $"DWMC",
        $"DWJB",
        $"SJDWBM",
        $"SJDWMC",
        $"BZID",
        $"SSDZ",
        $"CNW",
        $"ZXID",
        $"SSZX",
        $"XLXZ",
        $"ZYCD",
        $"QDDZ",
        $"ZDDZ",
        $"TYRQ",
        $"JSFS",
        $"ZDYXDL",
        $"JJDL",
        $"YXFHXE",
        $"EDSSGL",
        $"XLZCD",
        $"JKXLCD",
        $"DLXLCD",
        $"JKJXFS",
        $"TJRQ",
        $"ZDYGGL",
        $"ZDYGGL_SJ",
        $"ZXYGGL",
        $"ZXYGGL_SJ",
        $"ZDSZGL",
        $"ZDSZGL_SJ",
        $"ZXGLYS",
        $"ZXGLYS_SJ",
        $"ZDDL_A",
        $"ZDDL_A_SJ",
        $"ZXDL_A",
        $"ZXDL_A_SJ",
        $"ZDDL_B",
        $"ZDDL_B_SJ",
        $"ZXDL_B",
        $"ZXDL_B_SJ",
        $"ZDDL_C",
        $"ZDDL_C_SJ",
        $"ZXDL_C",
        $"ZXDL_C_SJ",
        $"ZXDY_A",
        $"ZXDY_A_SJ",
        $"ZXDY_B",
        $"ZXDY_B_SJ",
        $"ZXDY_C",
        $"ZXDY_C_SJ",
        $"ZDDY_A",
        $"ZDDY_A_SJ",
        $"ZDDY_B",
        $"ZDDY_B_SJ",
        $"ZDDY_C",
        $"ZDDY_C_SJ",
        $"PJFH",
        $"FHL",
        $"FZL",
        $"AXDYQXYCDS",
        $"AXDYQXSCDS",
        $"BXDYQXYCDS",
        $"BXDYQXSCDS",
        $"CXDYQXYCDS",
        $"CXDYQXSCDS",
        $"AXDLQXYCDS",
        $"AXDLQXSCDS",
        $"BXDLQXYCDS",
        $"BXDLQXSCDS",
        $"CXDLQXYCDS",
        $"CXDLQXSCDS",
        $"GLYSQXYCDS",
        $"GLYSQXSCDS",
        $"YGZGLQXYCDS",
        $"YGZGLQXSCDS",
        $"WGZGLQXYCDS",
        $"WGZGLQXSCDS")//,
//        expr("null ZXYGZDNL"),
//        expr("null ZXYGZDNL1"),
//        expr("null ZXYGZDNL2"),
//        expr("null ZXYGZDNL3"),
//        expr("null ZXYGZDNL4"),
//        expr("null FXYGZDNL"),
//        expr("null FXYGZDNL1"),
//        expr("null FXYGZDNL2"),
//        expr("null FXYGZDNL3"),
//        expr("null FXYGZDNL4"),
//        expr("null ZXWGZDNL"),
//        expr("null FXWGZDNL"),
//        expr("null XX1_R"),
//        expr("null XX4_R"),
//
//        expr("null AVG_F"),
//        expr("null FDL"),
//        expr("null GDL"),
//        expr("null SDL"),
//        expr("null XSL"),
//        expr("null POWEROFF_PIONT_CS"))
    res
  }

  def statRunStatusSeasonAndMore: DataFrame = {
    import hc.implicits._
    val dateFrom = DateFormatUtil.dateFrom(statDate, statCycle)
    val dateTo = DateFormatUtil.dateTo(dateFrom, statCycle)
    val ycds = DateFormatUtil.statDays(dateFrom, dateTo, statCycle) * 96l
    val weekDateTo = DateFormatUtil.dateDay(dateTo, 1)
    val tjrq = if (DateFormatUtil.STAT_CYCLE_WEEK.equals(statCycle)) s"'${dateFrom}' TJKSRQ, '${weekDateTo}' TJJSRQ\n"
    else s"'${statDate}' TJRQ\n"

    val sql1 = s"""select XLID,
       XLMC,
       YXBH,
       DYDJ,
       SSDS,
       YWDW,
       YWDWMC,
       DWBM,
       DWMC,
       DWJB,
       SJDWBM,
       SJDWMC,
       BZID,
       SSDZ,
       CNW,
       ZXID,
       SSZX,
       XLXZ,
       ZYCD,
       QDDZ,
       ZDDZ,
       TYRQ,
       JSFS,
       ZDYXDL,
       JJDL,
       YXFHXE,
       EDSSGL,
       XLZCD,
       JKXLCD,
       DLXLCD,
       JKJXFS,
       ${tjrq}
  from pwyw_kxyxzttj_y
 where tjrq >= '${dateFrom}'
   and tjrq < '${dateTo}'
 group by XLID,
       XLMC,
       YXBH,
       DYDJ,
       SSDS,
       YWDW,
       YWDWMC,
       DWBM,
       DWMC,
       DWJB,
       SJDWBM,
       SJDWMC,
       BZID,
       SSDZ,
       CNW,
       ZXID,
       SSZX,
       XLXZ,
       ZYCD,
       QDDZ,
       ZDDZ,
       TYRQ,
       JSFS,
       ZDYXDL,
       JJDL,
       YXFHXE,
       EDSSGL,
       XLZCD,
       JKXLCD,
       DLXLCD,
       JKJXFS"""
    val t1 = hc.read.jdbc(JdbcConnUtil.url, s"(${sql1})", JdbcConnUtil.connProp)

    val sql2 = s"""select XLID as ID,
       ZDYGGL,
       to_char(ZDYGGL_SJ,'yyyy-mm-dd hh24:mi:ss')||'.0' ZDYGGL_SJ,
       ZXYGGL,
       to_char(ZXYGGL_SJ,'yyyy-mm-dd hh24:mi:ss')||'.0' ZXYGGL_SJ,
       ZDSZGL,
       to_char(ZDSZGL_SJ,'yyyy-mm-dd hh24:mi:ss')||'.0' ZDSZGL_SJ,
       ZXGLYS,
       to_char(ZXGLYS_SJ,'yyyy-mm-dd hh24:mi:ss')||'.0' ZXGLYS_SJ,
       ZDDL_A,
       to_char(ZDDL_A_SJ,'yyyy-mm-dd hh24:mi:ss')||'.0' ZDDL_A_SJ,
       ZXDL_A,
       to_char(ZXDL_A_SJ,'yyyy-mm-dd hh24:mi:ss')||'.0' ZXDL_A_SJ,
       ZDDL_B,
       to_char(ZDDL_B_SJ,'yyyy-mm-dd hh24:mi:ss')||'.0' ZDDL_B_SJ,
       ZXDL_B,
       to_char(ZXDL_B_SJ,'yyyy-mm-dd hh24:mi:ss')||'.0' ZXDL_B_SJ,
       ZDDL_C,
       to_char(ZDDL_C_SJ,'yyyy-mm-dd hh24:mi:ss')||'.0' ZDDL_C_SJ,
       ZXDL_C,
       to_char(ZXDL_C_SJ,'yyyy-mm-dd hh24:mi:ss')||'.0' ZXDL_C_SJ,
       ZXDY_A,
       to_char(ZXDL_C_SJ,'yyyy-mm-dd hh24:mi:ss')||'.0' ZXDY_A_SJ,
       ZXDY_B,
       to_char(ZXDY_B_SJ,'yyyy-mm-dd hh24:mi:ss')||'.0' ZXDY_B_SJ,
       ZXDY_C,
       to_char(ZXDY_C_SJ,'yyyy-mm-dd hh24:mi:ss')||'.0' ZXDY_C_SJ,
       ZDDY_A,
       to_char(ZDDY_A_SJ,'yyyy-mm-dd hh24:mi:ss')||'.0' ZDDY_A_SJ,
       ZDDY_B,
       to_char(ZDDY_B_SJ,'yyyy-mm-dd hh24:mi:ss')||'.0' ZDDY_B_SJ,
       ZDDY_C,
       to_char(ZDDY_C_SJ,'yyyy-mm-dd hh24:mi:ss')||'.0' ZDDY_C_SJ,
       PJGL PJFH,
       FHL,
       FZL,
       AXDYQXYCDS,
       AXDYQXSCDS,
       BXDYQXYCDS,
       BXDYQXSCDS,
       CXDYQXYCDS,
       CXDYQXSCDS,
       AXDLQXYCDS,
       AXDLQXSCDS,
       BXDLQXYCDS,
       BXDLQXSCDS,
       CXDLQXYCDS,
       CXDLQXSCDS,
       GLYSQXYCDS,
       GLYSQXSCDS,
       YGZGLQXYCDS,
       YGZGLQXSCDS,
       WGZGLQXYCDS,
       WGZGLQXSCDS
  from pwyw_kxyxzttj_y
 where tjrq >= '${dateFrom}'
   and tjrq < '${dateTo}'"""

    val t2 = hc.read.jdbc(JdbcConnUtil.url, s"(${sql2})", JdbcConnUtil.connProp)
      .select($"ID",
        expr("cast(ZDYGGL as double)") as "ZDYGGL",
        $"ZDYGGL_SJ",
        expr("cast(ZXYGGL as double)") as "ZXYGGL",
        $"ZXYGGL_SJ",
        expr("cast(ZDSZGL as double)") as "ZDSZGL",
        $"ZDSZGL_SJ",
        expr("cast(ZXGLYS as double)") as "ZXGLYS",
        $"ZXGLYS_SJ",
        expr("cast(ZDDL_A as double)") as "ZDDL_A",
        $"ZDDL_A_SJ",
        expr("cast(ZXDL_A as double)") as "ZXDL_A",
        $"ZXDL_A_SJ",
        expr("cast(ZDDL_B as double)") as "ZDDL_B",
        $"ZDDL_B_SJ",
        expr("cast(ZXDL_B as double)") as "ZXDL_B",
        $"ZXDL_B_SJ",
        expr("cast(ZDDL_C as double)") as "ZDDL_C",
        $"ZDDL_C_SJ",
        expr("cast(ZXDL_C as double)") as "ZXDL_C",
        $"ZXDL_C_SJ",
        expr("cast(ZXDY_A as double)") as "ZXDY_A",
        $"ZXDY_A_SJ",
        expr("cast(ZXDY_B as double)") as "ZXDY_B",
        $"ZXDY_B_SJ",
        expr("cast(ZXDY_C as double)") as "ZXDY_C",
        $"ZXDY_C_SJ",
        expr("cast(ZDDY_A as double)") as "ZDDY_A",
        $"ZDDY_A_SJ",
        expr("cast(ZDDY_B as double)") as "ZDDY_B",
        $"ZDDY_B_SJ",
        expr("cast(ZDDY_C as double)") as "ZDDY_C",
        $"ZDDY_C_SJ",
        expr("cast(PJFH as double)") as "PJFH",
        expr("cast(FHL as double)") as "FHL",
        expr("cast(FZL as double)") as "FZL",
        expr("cast(AXDYQXYCDS as int)") as "AXDYQXYCDS",
        expr("cast(AXDYQXSCDS as int)") as "AXDYQXSCDS",
        expr("cast(BXDYQXYCDS as int)") as "BXDYQXYCDS",
        expr("cast(BXDYQXSCDS as int)") as "BXDYQXSCDS",
        expr("cast(CXDYQXYCDS as int)") as "CXDYQXYCDS",
        expr("cast(CXDYQXSCDS as int)") as "CXDYQXSCDS",
        expr("cast(AXDLQXYCDS as int)") as "AXDLQXYCDS",
        expr("cast(AXDLQXSCDS as int)") as "AXDLQXSCDS",
        expr("cast(BXDLQXYCDS as int)") as "BXDLQXYCDS",
        expr("cast(BXDLQXSCDS as int)") as "BXDLQXSCDS",
        expr("cast(CXDLQXYCDS as int)") as "CXDLQXYCDS",
        expr("cast(CXDLQXSCDS as int)") as "CXDLQXSCDS",
        expr("cast(GLYSQXYCDS as int)") as "GLYSQXYCDS",
        expr("cast(GLYSQXSCDS as int)") as "GLYSQXSCDS",
        expr("cast(YGZGLQXYCDS as int)") as "YGZGLQXYCDS",
        expr("cast(YGZGLQXSCDS as int)") as "YGZGLQXSCDS",
        expr("cast(WGZGLQXYCDS as int)") as "WGZGLQXYCDS",
        expr("cast(WGZGLQXSCDS as int)") as "WGZGLQXSCDS")

    val t2Group = t2.as[OracleRunStatusBean]
      .groupBy($"ID")
      .mapGroups((key, row) => {
        val seq = row.toSeq
        val ZDDY_A = if (seq.filter { c => c.ZDDY_A.isDefined }.isEmpty) null else seq.filter { c => c.ZDDY_A.isDefined }.maxBy { c => c.ZDDY_A }
        val ZDDY_B = if (seq.filter { c => c.ZDDY_B.isDefined }.isEmpty) null else seq.filter { c => c.ZDDY_B.isDefined }.maxBy { c => c.ZDDY_B }
        val ZDDY_C = if (seq.filter { c => c.ZDDY_C.isDefined }.isEmpty) null else seq.filter { c => c.ZDDY_C.isDefined }.maxBy { c => c.ZDDY_C }
        val ZDDL_A = if (seq.filter { c => c.ZDDL_A.isDefined }.isEmpty) null else seq.filter { c => c.ZDDL_A.isDefined }.maxBy { c => c.ZDDL_A }
        val ZDDL_B = if (seq.filter { c => c.ZDDL_B.isDefined }.isEmpty) null else seq.filter { c => c.ZDDL_B.isDefined }.maxBy { c => c.ZDDL_B }
        val ZDDL_C = if (seq.filter { c => c.ZDDL_C.isDefined }.isEmpty) null else seq.filter { c => c.ZDDL_C.isDefined }.maxBy { c => c.ZDDL_C }
        val ZDYGGL = if (seq.filter { c => c.ZDYGGL.isDefined }.isEmpty) null else seq.filter { c => c.ZDYGGL.isDefined }.maxBy { c => c.ZDYGGL }
        val ZDSZGL = if (seq.filter { c => c.ZDSZGL.isDefined }.isEmpty) null else seq.filter { c => c.ZDSZGL.isDefined }.maxBy { c => c.ZDSZGL }

        val ZXDY_A = if (seq.filter { c => c.ZXDY_A.isDefined }.isEmpty) null else seq.filter { c => c.ZXDY_A.isDefined }.minBy { c => c.ZXDY_A }
        val ZXDY_B = if (seq.filter { c => c.ZXDY_B.isDefined }.isEmpty) null else seq.filter { c => c.ZXDY_B.isDefined }.minBy { c => c.ZXDY_B }
        val ZXDY_C = if (seq.filter { c => c.ZXDY_C.isDefined }.isEmpty) null else seq.filter { c => c.ZXDY_C.isDefined }.minBy { c => c.ZXDY_C }
        val ZXDL_A = if (seq.filter { c => c.ZXDL_A.isDefined }.isEmpty) null else seq.filter { c => c.ZXDL_A.isDefined }.minBy { c => c.ZXDL_A }
        val ZXDL_B = if (seq.filter { c => c.ZXDL_B.isDefined }.isEmpty) null else seq.filter { c => c.ZXDL_B.isDefined }.minBy { c => c.ZXDL_B }
        val ZXDL_C = if (seq.filter { c => c.ZXDL_C.isDefined }.isEmpty) null else seq.filter { c => c.ZXDL_C.isDefined }.minBy { c => c.ZXDL_C }
        val ZXYGGL = if (seq.filter { c => c.ZXYGGL.isDefined }.isEmpty) null else seq.filter { c => c.ZXYGGL.isDefined }.minBy { c => c.ZXYGGL }

        val FZL = if (seq.filter { c => c.FZL.isDefined }.isEmpty) null else seq.filter { c => c.FZL.isDefined }.maxBy { c => c.FZL }

        val count = seq.count { c => c.ID.isDefined }

        val sumBean = seq.reduce((a, b) => {
          new OracleRunStatusBean(a.ID,
            a.ZDYGGL,
            a.ZDYGGL_SJ,
            a.ZXYGGL,
            a.ZXYGGL_SJ,
            a.ZDSZGL,
            a.ZDSZGL_SJ,
            a.ZXGLYS,
            a.ZXGLYS_SJ,
            a.ZDDL_A,
            a.ZDDL_A_SJ,
            a.ZXDL_A,
            a.ZXDL_A_SJ,
            a.ZDDL_B,
            a.ZDDL_B_SJ,
            a.ZXDL_B,
            a.ZXDL_B_SJ,
            a.ZDDL_C,
            a.ZDDL_C_SJ,
            a.ZXDL_C,
            a.ZXDL_C_SJ,
            a.ZXDY_A,
            a.ZXDY_A_SJ,
            a.ZXDY_B,
            a.ZXDY_B_SJ,
            a.ZXDY_C,
            a.ZXDY_C_SJ,
            a.ZDDY_A,
            a.ZDDY_A_SJ,
            a.ZDDY_B,
            a.ZDDY_B_SJ,
            a.ZDDY_C,
            a.ZDDY_C_SJ,
            if (a.PJFH.isDefined && b.PJFH.isDefined) Some(a.PJFH.get + b.PJFH.get) else None,
            a.FHL,
            if (a.AXDYQXYCDS.isDefined && b.AXDYQXYCDS.isDefined) Some(a.AXDYQXYCDS.get + b.AXDYQXYCDS.get) else None,
            if (a.AXDYQXSCDS.isDefined && b.AXDYQXSCDS.isDefined) Some(a.AXDYQXSCDS.get + b.AXDYQXSCDS.get) else None,
            if (a.BXDYQXYCDS.isDefined && b.BXDYQXYCDS.isDefined) Some(a.BXDYQXYCDS.get + b.BXDYQXYCDS.get) else None,
            if (a.BXDYQXSCDS.isDefined && b.BXDYQXSCDS.isDefined) Some(a.BXDYQXSCDS.get + b.BXDYQXSCDS.get) else None,
            if (a.CXDYQXYCDS.isDefined && b.CXDYQXYCDS.isDefined) Some(a.CXDYQXYCDS.get + b.CXDYQXYCDS.get) else None,
            if (a.CXDYQXSCDS.isDefined && b.CXDYQXSCDS.isDefined) Some(a.CXDYQXSCDS.get + b.CXDYQXSCDS.get) else None,
            if (a.AXDLQXYCDS.isDefined && b.AXDLQXYCDS.isDefined) Some(a.AXDLQXYCDS.get + b.AXDLQXYCDS.get) else None,
            if (a.AXDLQXSCDS.isDefined && b.AXDLQXSCDS.isDefined) Some(a.AXDLQXSCDS.get + b.AXDLQXSCDS.get) else None,
            if (a.BXDLQXYCDS.isDefined && b.BXDLQXYCDS.isDefined) Some(a.BXDLQXYCDS.get + b.BXDLQXYCDS.get) else None,
            if (a.BXDLQXSCDS.isDefined && b.BXDLQXSCDS.isDefined) Some(a.BXDLQXSCDS.get + b.BXDLQXSCDS.get) else None,
            if (a.CXDLQXYCDS.isDefined && b.CXDLQXYCDS.isDefined) Some(a.CXDLQXYCDS.get + b.CXDLQXYCDS.get) else None,
            if (a.CXDLQXSCDS.isDefined && b.CXDLQXSCDS.isDefined) Some(a.CXDLQXSCDS.get + b.CXDLQXSCDS.get) else None,
            if (a.GLYSQXYCDS.isDefined && b.GLYSQXYCDS.isDefined) Some(a.GLYSQXYCDS.get + b.GLYSQXYCDS.get) else None,
            if (a.GLYSQXSCDS.isDefined && b.GLYSQXSCDS.isDefined) Some(a.GLYSQXSCDS.get + b.GLYSQXSCDS.get) else None,
            if (a.YGZGLQXYCDS.isDefined && b.YGZGLQXYCDS.isDefined) Some(a.YGZGLQXYCDS.get + b.YGZGLQXYCDS.get) else None,
            if (a.YGZGLQXSCDS.isDefined && b.YGZGLQXSCDS.isDefined) Some(a.YGZGLQXSCDS.get + b.YGZGLQXSCDS.get) else None,
            if (a.WGZGLQXYCDS.isDefined && b.WGZGLQXYCDS.isDefined) Some(a.WGZGLQXYCDS.get + b.WGZGLQXYCDS.get) else None,
            if (a.WGZGLQXSCDS.isDefined && b.WGZGLQXSCDS.isDefined) Some(a.WGZGLQXSCDS.get + b.WGZGLQXSCDS.get) else None,
            None)
        })

        val pjfh: java.lang.Double = if (sumBean.PJFH.isDefined) sumBean.PJFH.get / count else null

        val fhl: java.lang.Double = if (ZDYGGL != null && ZDYGGL.ZDYGGL.isDefined && pjfh != null) pjfh / ZDYGGL.ZDYGGL.get else null

        new OracleRunStatusBean(Some(key.getAs[String](0)),
          if (ZDYGGL != null) ZDYGGL.ZDYGGL else None,
          if (ZDYGGL != null) ZDYGGL.ZDYGGL_SJ else None,
          if (ZXYGGL != null) ZXYGGL.ZXYGGL else None,
          if (ZXYGGL != null) ZXYGGL.ZXYGGL_SJ else None,
          if (ZDSZGL != null) ZDSZGL.ZDSZGL else None,
          if (ZDSZGL != null) ZDSZGL.ZDSZGL_SJ else None,
          None,
          None,
          //          if (minF != null) minF.F else None,
          //          if (minF != null) minF.DATA_TIME else None,
          if (ZDDL_A != null) ZDDL_A.ZDDL_A else None,
          if (ZDDL_A != null) ZDDL_A.ZDDL_A_SJ else None,
          if (ZXDL_A != null) ZXDL_A.ZXDL_A else None,
          if (ZXDL_A != null) ZXDL_A.ZXDL_A_SJ else None,
          if (ZDDL_B != null) ZDDL_B.ZDDL_B else None,
          if (ZDDL_B != null) ZDDL_B.ZDDL_B_SJ else None,
          if (ZXDL_B != null) ZXDL_B.ZXDL_B else None,
          if (ZXDL_B != null) ZXDL_B.ZXDL_B_SJ else None,
          if (ZDDL_C != null) ZDDL_C.ZDDL_C else None,
          if (ZDDL_C != null) ZDDL_C.ZDDL_C_SJ else None,
          if (ZXDL_C != null) ZXDL_C.ZXDL_C else None,
          if (ZXDL_C != null) ZXDL_C.ZXDL_C_SJ else None,
          if (ZXDY_A != null) ZXDY_A.ZXDY_A else None,
          if (ZXDY_A != null) ZXDY_A.ZXDY_A_SJ else None,
          if (ZXDY_B != null) ZXDY_B.ZXDY_B else None,
          if (ZXDY_B != null) ZXDY_B.ZXDY_B_SJ else None,
          if (ZXDY_C != null) ZXDY_C.ZXDY_C else None,
          if (ZXDY_C != null) ZXDY_C.ZXDY_C_SJ else None,
          if (ZDDY_A != null) ZDDY_A.ZDDY_A else None,
          if (ZDDY_A != null) ZDDY_A.ZDDY_A_SJ else None,
          if (ZDDY_B != null) ZDDY_B.ZDDY_B else None,
          if (ZDDY_B != null) ZDDY_B.ZDDY_B_SJ else None,
          if (ZDDY_C != null) ZDDY_C.ZDDY_C else None,
          if (ZDDY_C != null) ZDDY_C.ZDDY_C_SJ else None,
          if (pjfh != null) Some(pjfh) else None,
          if (fhl != null) Some(fhl) else None,
          sumBean.AXDYQXYCDS,
          sumBean.AXDYQXSCDS,
          sumBean.BXDYQXYCDS,
          sumBean.BXDYQXSCDS,
          sumBean.CXDYQXYCDS,
          sumBean.CXDYQXSCDS,
          sumBean.AXDLQXYCDS,
          sumBean.AXDLQXSCDS,
          sumBean.BXDLQXYCDS,
          sumBean.BXDLQXSCDS,
          sumBean.CXDLQXYCDS,
          sumBean.CXDLQXSCDS,
          sumBean.GLYSQXYCDS,
          sumBean.GLYSQXSCDS,
          sumBean.YGZGLQXYCDS,
          sumBean.YGZGLQXSCDS,
          sumBean.WGZGLQXYCDS,
          sumBean.WGZGLQXSCDS,
          if (FZL != null) FZL.FZL else None)
      }).toDF().withColumnRenamed("ID", "XLID")

    val joinData = t1.join(t2Group, Seq("XLID"), "left_outer")
    val res = if (DateFormatUtil.STAT_CYCLE_WEEK.equals(statCycle))
      joinData.select($"XLID",
        $"XLMC",
        $"YXBH",
        $"DYDJ",
        $"SSDS",
        $"YWDW",
        $"YWDWMC",
        $"DWBM",
        $"DWMC",
        $"DWJB",
        $"SJDWBM",
        $"SJDWMC",
        $"BZID",
        $"SSDZ",
        $"CNW",
        $"ZXID",
        $"SSZX",
        $"XLXZ",
        $"ZYCD",
        $"QDDZ",
        $"ZDDZ",
        $"TYRQ",
        $"JSFS",
        $"ZDYXDL",
        $"JJDL",
        $"YXFHXE",
        $"EDSSGL",
        $"XLZCD",
        $"JKXLCD",
        $"DLXLCD",
        $"JKJXFS",
        $"TJKSRQ",
        $"TJJSRQ",
        $"ZDYGGL",
        $"ZDYGGL_SJ",
        $"ZXYGGL",
        $"ZXYGGL_SJ",
        $"ZDSZGL",
        $"ZDSZGL_SJ",
        $"ZXGLYS",
        $"ZXGLYS_SJ",
        $"ZDDL_A",
        $"ZDDL_A_SJ",
        $"ZXDL_A",
        $"ZXDL_A_SJ",
        $"ZDDL_B",
        $"ZDDL_B_SJ",
        $"ZXDL_B",
        $"ZXDL_B_SJ",
        $"ZDDL_C",
        $"ZDDL_C_SJ",
        $"ZXDL_C",
        $"ZXDL_C_SJ",
        $"ZXDY_A",
        $"ZXDY_A_SJ",
        $"ZXDY_B",
        $"ZXDY_B_SJ",
        $"ZXDY_C",
        $"ZXDY_C_SJ",
        $"ZDDY_A",
        $"ZDDY_A_SJ",
        $"ZDDY_B",
        $"ZDDY_B_SJ",
        $"ZDDY_C",
        $"ZDDY_C_SJ",
        $"PJFH",
        $"FHL",
        $"FZL",
        $"AXDYQXYCDS",
        $"AXDYQXSCDS",
        $"BXDYQXYCDS",
        $"BXDYQXSCDS",
        $"CXDYQXYCDS",
        $"CXDYQXSCDS",
        $"AXDLQXYCDS",
        $"AXDLQXSCDS",
        $"BXDLQXYCDS",
        $"BXDLQXSCDS",
        $"CXDLQXYCDS",
        $"CXDLQXSCDS",
        $"GLYSQXYCDS",
        $"GLYSQXSCDS",
        $"YGZGLQXYCDS",
        $"YGZGLQXSCDS",
        $"WGZGLQXYCDS",
        $"WGZGLQXSCDS")//,
//        expr("null ZXYGZDNL"),
//        expr("null ZXYGZDNL1"),
//        expr("null ZXYGZDNL2"),
//        expr("null ZXYGZDNL3"),
//        expr("null ZXYGZDNL4"),
//        expr("null FXYGZDNL"),
//        expr("null FXYGZDNL1"),
//        expr("null FXYGZDNL2"),
//        expr("null FXYGZDNL3"),
//        expr("null FXYGZDNL4"),
//        expr("null ZXWGZDNL"),
//        expr("null FXWGZDNL"),
//        expr("null XX1_R"),
//        expr("null XX4_R"),
//
//        expr("null AVG_F"),
//        expr("null FDL"),
//        expr("null GDL"),
//        expr("null SDL"),
//        expr("null XSL"),
//        expr("null POWEROFF_PIONT_CS"))
    else
      joinData.select($"XLID",
        $"XLMC",
        $"YXBH",
        $"DYDJ",
        $"SSDS",
        $"YWDW",
        $"YWDWMC",
        $"DWBM",
        $"DWMC",
        $"DWJB",
        $"SJDWBM",
        $"SJDWMC",
        $"BZID",
        $"SSDZ",
        $"CNW",
        $"ZXID",
        $"SSZX",
        $"XLXZ",
        $"ZYCD",
        $"QDDZ",
        $"ZDDZ",
        $"TYRQ",
        $"JSFS",
        $"ZDYXDL",
        $"JJDL",
        $"YXFHXE",
        $"EDSSGL",
        $"XLZCD",
        $"JKXLCD",
        $"DLXLCD",
        $"JKJXFS",
        $"TJRQ",
        $"ZDYGGL",
        $"ZDYGGL_SJ",
        $"ZXYGGL",
        $"ZXYGGL_SJ",
        $"ZDSZGL",
        $"ZDSZGL_SJ",
        $"ZXGLYS",
        $"ZXGLYS_SJ",
        $"ZDDL_A",
        $"ZDDL_A_SJ",
        $"ZXDL_A",
        $"ZXDL_A_SJ",
        $"ZDDL_B",
        $"ZDDL_B_SJ",
        $"ZXDL_B",
        $"ZXDL_B_SJ",
        $"ZDDL_C",
        $"ZDDL_C_SJ",
        $"ZXDL_C",
        $"ZXDL_C_SJ",
        $"ZXDY_A",
        $"ZXDY_A_SJ",
        $"ZXDY_B",
        $"ZXDY_B_SJ",
        $"ZXDY_C",
        $"ZXDY_C_SJ",
        $"ZDDY_A",
        $"ZDDY_A_SJ",
        $"ZDDY_B",
        $"ZDDY_B_SJ",
        $"ZDDY_C",
        $"ZDDY_C_SJ",
        $"PJFH",
        $"FHL",
        $"FZL",
        $"AXDYQXYCDS",
        $"AXDYQXSCDS",
        $"BXDYQXYCDS",
        $"BXDYQXSCDS",
        $"CXDYQXYCDS",
        $"CXDYQXSCDS",
        $"AXDLQXYCDS",
        $"AXDLQXSCDS",
        $"BXDLQXYCDS",
        $"BXDLQXSCDS",
        $"CXDLQXYCDS",
        $"CXDLQXSCDS",
        $"GLYSQXYCDS",
        $"GLYSQXSCDS",
        $"YGZGLQXYCDS",
        $"YGZGLQXSCDS",
        $"WGZGLQXYCDS",
        $"WGZGLQXSCDS")//,
//        expr("null ZXYGZDNL"),
//        expr("null ZXYGZDNL1"),
//        expr("null ZXYGZDNL2"),
//        expr("null ZXYGZDNL3"),
//        expr("null ZXYGZDNL4"),
//        expr("null FXYGZDNL"),
//        expr("null FXYGZDNL1"),
//        expr("null FXYGZDNL2"),
//        expr("null FXYGZDNL3"),
//        expr("null FXYGZDNL4"),
//        expr("null ZXWGZDNL"),
//        expr("null FXWGZDNL"),
//        expr("null XX1_R"),
//        expr("null XX4_R"),
//
//        expr("null AVG_F"),
//        expr("null FDL"),
//        expr("null GDL"),
//        expr("null SDL"),
//        expr("null XSL"),
//        expr("null POWEROFF_PIONT_CS"))
    res
  }

  def statRunStatus: DataFrame = {
    val df = if (DateFormatUtil.STAT_CYCLE_DAY.equals(statCycle)) {
      statRunStatusDay
    } else if (DateFormatUtil.STAT_CYCLE_WEEK.equals(statCycle) || DateFormatUtil.STAT_CYCLE_MON.equals(statCycle)) {
      statRunStatusWeekAndMon
    } else {
      statRunStatusSeasonAndMore
    }
    df
  }

  def statExcept: DataFrame = {
    val df = if (DateFormatUtil.STAT_CYCLE_DAY.equals(statCycle)) {
      statExceptDay
    } else if (DateFormatUtil.STAT_CYCLE_WEEK.equals(statCycle) || DateFormatUtil.STAT_CYCLE_MON.equals(statCycle)) {
      statExceptWeekAndMon
    } else {
      statExceptSeasonAndMore
    }
    df
  }

  def statSSClass: DataFrame = {
    import hc.implicits._
    val dateFrom = DateFormatUtil.dateFrom(statDate, statCycle)
    val dateTo = DateFormatUtil.dateTo(dateFrom, statCycle)
    val weekDateTo = DateFormatUtil.dateDay(dateTo, 1)
    val tjrq = if (DateFormatUtil.STAT_CYCLE_WEEK.equals(statCycle)) s"'${dateFrom}' TJKSRQ, '${weekDateTo}' TJJSRQ\n"
    else s"'${statDate}' TJRQ\n"

    val sql = s"""SELECT t.OBJ_ID,
         t.SSDS DWBM,
         t2.PMS_DWCJ DWJB,
         t2.PMS_DWMC DWMC,
         t2.sjdwid SJDWBM,
         t2.SJDWMC,
         t.CNW,
         t.XLXZ,
         t.EDRLDJ,
         t3.YX_XLBS,
         t4.XLID,
         t4.GJBM,
         t4.DQGJFSCS,
         t4.DQGJFSSC,
         t4.SJDJ,
         ${tjrq}
   FROM 
        (SELECT OBJ_ID,
            SSDS,
            CASE WHEN edssgl < 100 THEN 1
              WHEN edssgl >= 100 AND edssgl < 315 THEN 2
              WHEN edssgl >= 315 AND edssgl < 500 THEN 3
              ELSE 4 END EDRLDJ,
            xlxz, 
            CASE WHEN SFNW = '1' THEN '3'
              ELSE '2' END CNW
        FROM pwyw_arch.T_SB_ZWYC_XL
        WHERE SFNW is NOT null
        union all
        SELECT OBJ_ID,
            b.sjdwid SSDS,
            CASE WHEN edssgl < 100 THEN 1
              WHEN edssgl >= 100 AND edssgl < 315 THEN 2
              WHEN edssgl >= 315 AND edssgl < 500 THEN 3
              ELSE 4 END EDRLDJ,
            xlxz, 
            CASE WHEN SFNW = '1' THEN '3'
              ELSE '2' END CNW
        FROM pwyw_arch.T_SB_ZWYC_XL a join pwyw_arch.st_pms_yx_dw b ON (a.SSDS = b.pms_dwid)
        WHERE SFNW is NOT null) t
  LEFT JOIN pwyw_arch.ST_PMS_YX_DW t2
    ON (t.ssds = t2.pms_dwid)
  LEFT JOIN pwyw_arch.st_bdz_xl t3
    ON (t.OBJ_ID = t3.pms_xlbs)
  LEFT JOIN (select * from pwyw.PWYW_KXYDYC where dt >= '${dateFrom}' and dt < '${dateTo}' and dqgjfssj >= '${dateFrom}' and dqgjfssj < '${dateTo}') t4 on(t.obj_id = t4.XLID)"""

    val data0 = hc.sql(sql).distinct()

    val dataR = data0.filter("GJBM in('00130','00131','00132')").drop("GJBM").withColumn("GJBM", lit("0013R"))
    val dataS = data0.filter("GJBM in('00133','00134','00135')").drop("GJBM").withColumn("GJBM", lit("0013S"))
    val dataT = data0.filter("GJBM in('00136','00137','00138')").drop("GJBM").withColumn("GJBM", lit("0013T"))

    val tab = if (DateFormatUtil.STAT_CYCLE_WEEK.equals(statCycle))
      data0.select("OBJ_ID", "DWBM", "DWJB", "DWMC", "SJDWBM", "SJDWMC", "CNW", "XLXZ", "EDRLDJ", "XLID", "YX_XLBS", "DQGJFSCS", "DQGJFSSC", "TJKSRQ", "TJJSRQ", "GJBM", "SJDJ")
        .unionAll(dataR.select("OBJ_ID", "DWBM", "DWJB", "DWMC", "SJDWBM", "SJDWMC", "CNW", "XLXZ", "EDRLDJ", "XLID", "YX_XLBS", "DQGJFSCS", "DQGJFSSC", "TJKSRQ", "TJJSRQ", "GJBM", "SJDJ"))
        .unionAll(dataS.select("OBJ_ID", "DWBM", "DWJB", "DWMC", "SJDWBM", "SJDWMC", "CNW", "XLXZ", "EDRLDJ", "XLID", "YX_XLBS", "DQGJFSCS", "DQGJFSSC", "TJKSRQ", "TJJSRQ", "GJBM", "SJDJ"))
        .unionAll(dataT.select("OBJ_ID", "DWBM", "DWJB", "DWMC", "SJDWBM", "SJDWMC", "CNW", "XLXZ", "EDRLDJ", "XLID", "YX_XLBS", "DQGJFSCS", "DQGJFSSC", "TJKSRQ", "TJJSRQ", "GJBM", "SJDJ"))
    else
      data0.select("OBJ_ID", "DWBM", "DWJB", "DWMC", "SJDWBM", "SJDWMC", "CNW", "XLXZ", "EDRLDJ", "XLID", "YX_XLBS", "DQGJFSCS", "DQGJFSSC", "TJRQ", "GJBM", "SJDJ")
        .unionAll(dataR.select("OBJ_ID", "DWBM", "DWJB", "DWMC", "SJDWBM", "SJDWMC", "CNW", "XLXZ", "EDRLDJ", "XLID", "YX_XLBS", "DQGJFSCS", "DQGJFSSC", "TJRQ", "GJBM", "SJDJ"))
        .unionAll(dataS.select("OBJ_ID", "DWBM", "DWJB", "DWMC", "SJDWBM", "SJDWMC", "CNW", "XLXZ", "EDRLDJ", "XLID", "YX_XLBS", "DQGJFSCS", "DQGJFSSC", "TJRQ", "GJBM", "SJDJ"))
        .unionAll(dataT.select("OBJ_ID", "DWBM", "DWJB", "DWMC", "SJDWBM", "SJDWMC", "CNW", "XLXZ", "EDRLDJ", "XLID", "YX_XLBS", "DQGJFSCS", "DQGJFSSC", "TJRQ", "GJBM", "SJDJ"))

    val dfGroup1 = if (DateFormatUtil.STAT_CYCLE_WEEK.equals(statCycle))
      tab.groupBy("DWBM", "DWMC", "DWJB", "SJDWBM", "SJDWMC", "CNW", "XLXZ", "EDRLDJ", "TJKSRQ", "TJJSRQ")
    else tab.groupBy("DWBM", "DWMC", "DWJB", "SJDWBM", "SJDWMC", "CNW", "XLXZ", "EDRLDJ", "TJRQ")
    val dfGroup2 = if (DateFormatUtil.STAT_CYCLE_WEEK.equals(statCycle))
      tab.groupBy("DWBM", "DWMC", "DWJB", "SJDWBM", "SJDWMC", "CNW", "XLXZ", "EDRLDJ", "TJKSRQ", "TJJSRQ", "GJBM", "SJDJ")
    else tab.groupBy("DWBM", "DWMC", "DWJB", "SJDWBM", "SJDWMC", "CNW", "XLXZ", "EDRLDJ", "TJRQ", "GJBM", "SJDJ")

    val df1 = dfGroup1.agg(countDistinct("OBJ_ID") as ("PMSKXSL"),
      countDistinct("YX_XLBS") as ("CJKXSL"))
    val df2 = dfGroup2.agg(countDistinct(expr("XLID")) as ("GZTS"),
      sum("DQGJFSCS") as ("GZCS"),
      sum("DQGJFSSC") as ("GZSC"))
    val df = if (DateFormatUtil.STAT_CYCLE_WEEK.equals(statCycle))
      df1.join(df2, Seq("DWBM", "DWMC", "DWJB", "SJDWBM", "SJDWMC", "CNW", "XLXZ", "EDRLDJ", "TJKSRQ", "TJJSRQ"))
    else df1.join(df2, Seq("DWBM", "DWMC", "DWJB", "SJDWBM", "SJDWMC", "CNW", "XLXZ", "EDRLDJ", "TJRQ"))

    val res = if (DateFormatUtil.STAT_CYCLE_WEEK.equals(statCycle))
      df.filter($"GJBM".isNotNull).select($"DWBM", $"DWMC", $"DWJB", $"SJDWBM", $"SJDWMC",
        $"CNW", $"XLXZ", $"EDRLDJ",
        $"TJKSRQ", $"TJJSRQ",
        $"GJBM", $"SJDJ",
        $"PMSKXSL", $"CJKXSL", $"GZTS",
        expr("round(GZTS * 100/PMSKXSL,2) GZZB"),
        $"GZCS", $"GZSC")
    else
      df.filter($"GJBM".isNotNull).select($"DWBM", $"DWMC", $"DWJB", $"SJDWBM", $"SJDWMC",
        $"CNW", $"XLXZ", $"EDRLDJ",
        $"TJRQ",
        $"GJBM", $"SJDJ",
        $"PMSKXSL", $"CJKXSL", $"GZTS",
        expr("round(GZTS * 100/PMSKXSL,2) GZZB"),
        $"GZCS", $"GZSC")

    val bzres = statBZClass

    res.unionAll(bzres)
  }

  def statBZClass: DataFrame = {
    import hc.implicits._
    val dateFrom = DateFormatUtil.dateFrom(statDate, statCycle)
    val dateTo = DateFormatUtil.dateTo(dateFrom, statCycle)
    val weekDateTo = DateFormatUtil.dateDay(dateTo, 1)
    val tjrq = if (DateFormatUtil.STAT_CYCLE_WEEK.equals(statCycle)) s"'${dateFrom}' TJKSRQ, '${weekDateTo}' TJJSRQ\n"
    else s"'${statDate}' TJRQ\n"

    val sql = s"""SELECT t.OBJ_ID,
         t.WHBZ DWBM,
         t2.PMS_DWCJ DWJB,
         t2.PMS_DWMC DWMC,
         t2.sjdwid SJDWBM,
         t2.SJDWMC,
         t.CNW,
         t.XLXZ,
         t.EDRLDJ,
         t3.YX_XLBS,
         t4.XLID,
         t4.GJBM,
         t4.DQGJFSCS,
         t4.DQGJFSSC,
         t4.SJDJ,
         ${tjrq}
   FROM 
        (SELECT OBJ_ID,
            WHBZ,
            CASE WHEN edssgl < 100 THEN 1
              WHEN edssgl >= 100 AND edssgl < 315 THEN 2
              WHEN edssgl >= 315 AND edssgl < 500 THEN 3
              ELSE 4 END EDRLDJ,
            xlxz, 
            CASE WHEN SFNW = '1' THEN '3'
              ELSE '2' END CNW
        FROM pwyw_arch.T_SB_ZWYC_XL
        WHERE SFNW is NOT null
        union all
        SELECT OBJ_ID,
            b.sjdwid WHBZ,
            CASE WHEN edssgl < 100 THEN 1
              WHEN edssgl >= 100 AND edssgl < 315 THEN 2
              WHEN edssgl >= 315 AND edssgl < 500 THEN 3
              ELSE 4 END EDRLDJ,
            xlxz, 
            CASE WHEN SFNW = '1' THEN '3'
              ELSE '2' END CNW
        FROM pwyw_arch.T_SB_ZWYC_XL a join pwyw_arch.st_pms_yx_dw b ON (a.WHBZ = b.pms_dwid)
        WHERE SFNW is NOT null) t
  LEFT JOIN pwyw_arch.ST_PMS_YX_DW t2
    ON (t.WHBZ = t2.pms_dwid)
  LEFT JOIN pwyw_arch.st_bdz_xl t3
    ON (t.OBJ_ID = t3.pms_xlbs)
  LEFT JOIN (select * from pwyw.PWYW_KXYDYC where dt >= '${dateFrom}' and dt < '${dateTo}' and dqgjfssj >= '${dateFrom}' and dqgjfssj < '${dateTo}') t4 on(t.obj_id = t4.XLID)"""

    val data0 = hc.sql(sql).distinct()

    val dataR = data0.filter("GJBM in('00130','00131','00132')").drop("GJBM").withColumn("GJBM", lit("0013R"))
    val dataS = data0.filter("GJBM in('00133','00134','00135')").drop("GJBM").withColumn("GJBM", lit("0013S"))
    val dataT = data0.filter("GJBM in('00136','00137','00138')").drop("GJBM").withColumn("GJBM", lit("0013T"))

    val tab = if (DateFormatUtil.STAT_CYCLE_WEEK.equals(statCycle))
      data0.select("OBJ_ID", "DWBM", "DWJB", "DWMC", "SJDWBM", "SJDWMC", "CNW", "XLXZ", "EDRLDJ", "XLID", "YX_XLBS", "DQGJFSCS", "DQGJFSSC", "TJKSRQ", "TJJSRQ", "GJBM", "SJDJ")
        .unionAll(dataR.select("OBJ_ID", "DWBM", "DWJB", "DWMC", "SJDWBM", "SJDWMC", "CNW", "XLXZ", "EDRLDJ", "XLID", "YX_XLBS", "DQGJFSCS", "DQGJFSSC", "TJKSRQ", "TJJSRQ", "GJBM", "SJDJ"))
        .unionAll(dataS.select("OBJ_ID", "DWBM", "DWJB", "DWMC", "SJDWBM", "SJDWMC", "CNW", "XLXZ", "EDRLDJ", "XLID", "YX_XLBS", "DQGJFSCS", "DQGJFSSC", "TJKSRQ", "TJJSRQ", "GJBM", "SJDJ"))
        .unionAll(dataT.select("OBJ_ID", "DWBM", "DWJB", "DWMC", "SJDWBM", "SJDWMC", "CNW", "XLXZ", "EDRLDJ", "XLID", "YX_XLBS", "DQGJFSCS", "DQGJFSSC", "TJKSRQ", "TJJSRQ", "GJBM", "SJDJ"))
    else
      data0.select("OBJ_ID", "DWBM", "DWJB", "DWMC", "SJDWBM", "SJDWMC", "CNW", "XLXZ", "EDRLDJ", "XLID", "YX_XLBS", "DQGJFSCS", "DQGJFSSC", "TJRQ", "GJBM", "SJDJ")
        .unionAll(dataR.select("OBJ_ID", "DWBM", "DWJB", "DWMC", "SJDWBM", "SJDWMC", "CNW", "XLXZ", "EDRLDJ", "XLID", "YX_XLBS", "DQGJFSCS", "DQGJFSSC", "TJRQ", "GJBM", "SJDJ"))
        .unionAll(dataS.select("OBJ_ID", "DWBM", "DWJB", "DWMC", "SJDWBM", "SJDWMC", "CNW", "XLXZ", "EDRLDJ", "XLID", "YX_XLBS", "DQGJFSCS", "DQGJFSSC", "TJRQ", "GJBM", "SJDJ"))
        .unionAll(dataT.select("OBJ_ID", "DWBM", "DWJB", "DWMC", "SJDWBM", "SJDWMC", "CNW", "XLXZ", "EDRLDJ", "XLID", "YX_XLBS", "DQGJFSCS", "DQGJFSSC", "TJRQ", "GJBM", "SJDJ"))

    val dfGroup1 = if (DateFormatUtil.STAT_CYCLE_WEEK.equals(statCycle))
      tab.groupBy("DWBM", "DWMC", "DWJB", "SJDWBM", "SJDWMC", "CNW", "XLXZ", "EDRLDJ", "TJKSRQ", "TJJSRQ")
    else tab.groupBy("DWBM", "DWMC", "DWJB", "SJDWBM", "SJDWMC", "CNW", "XLXZ", "EDRLDJ", "TJRQ")
    val dfGroup2 = if (DateFormatUtil.STAT_CYCLE_WEEK.equals(statCycle))
      tab.groupBy("DWBM", "DWMC", "DWJB", "SJDWBM", "SJDWMC", "CNW", "XLXZ", "EDRLDJ", "TJKSRQ", "TJJSRQ", "GJBM", "SJDJ")
    else tab.groupBy("DWBM", "DWMC", "DWJB", "SJDWBM", "SJDWMC", "CNW", "XLXZ", "EDRLDJ", "TJRQ", "GJBM", "SJDJ")

    val df1 = dfGroup1.agg(countDistinct("OBJ_ID") as ("PMSKXSL"),
      countDistinct("YX_XLBS") as ("CJKXSL"))
    val df2 = dfGroup2.agg(countDistinct(expr("XLID")) as ("GZTS"),
      sum("DQGJFSCS") as ("GZCS"),
      sum("DQGJFSSC") as ("GZSC"))
    val df = if (DateFormatUtil.STAT_CYCLE_WEEK.equals(statCycle))
      df1.join(df2, Seq("DWBM", "DWMC", "DWJB", "SJDWBM", "SJDWMC", "CNW", "XLXZ", "EDRLDJ", "TJKSRQ", "TJJSRQ"))
    else df1.join(df2, Seq("DWBM", "DWMC", "DWJB", "SJDWBM", "SJDWMC", "CNW", "XLXZ", "EDRLDJ", "TJRQ"))

    val res = if (DateFormatUtil.STAT_CYCLE_WEEK.equals(statCycle))
      df.filter($"GJBM".isNotNull).select($"DWBM", $"DWMC", $"DWJB", $"SJDWBM", $"SJDWMC",
        $"CNW", $"XLXZ", $"EDRLDJ",
        $"TJKSRQ", $"TJJSRQ",
        $"GJBM", $"SJDJ",
        $"PMSKXSL", $"CJKXSL", $"GZTS",
        expr("round(GZTS * 100/PMSKXSL,2) GZZB"),
        $"GZCS", $"GZSC")
    else
      df.filter($"GJBM".isNotNull).select($"DWBM", $"DWMC", $"DWJB", $"SJDWBM", $"SJDWMC",
        $"CNW", $"XLXZ", $"EDRLDJ",
        $"TJRQ",
        $"GJBM", $"SJDJ",
        $"PMSKXSL", $"CJKXSL", $"GZTS",
        expr("round(GZTS * 100/PMSKXSL,2) GZZB"),
        $"GZCS", $"GZSC")
    res
  }

  def statSSGatherSuccRateDay: DataFrame = {
    import hc.implicits._
    val dateFrom = DateFormatUtil.dateFrom(statDate, DateFormatUtil.STAT_CYCLE_DAY)
    val dateTo = DateFormatUtil.dateTo(dateFrom, DateFormatUtil.STAT_CYCLE_DAY)

    val ycds = DateFormatUtil.statDays(dateFrom, dateTo, DateFormatUtil.STAT_CYCLE_DAY) * 96l

    val tjrq = s"'${statDate}' TJRQ\n"

    val sql = s"""SELECT t.OBJ_ID,
                         t.SSDS DWBM,
                         t2.PMS_DWCJ DWJB,
                         t.CNW,
                         t.XLXZ,
                         t3.YX_XLBS,
                         t4.XLID,
                         t4.SJSJ DATA_TIME,
                         t4.UAB UA,
                         t4.UBC UB,
                         t4.UCA UC,
                         t4.IA,
                         t4.IB,
                         t4.IC,
                         t4.P,
                         t4.Q,
                         NULL F,
                         ${tjrq}
                FROM 
                  (SELECT OBJ_ID,
                         SSDS,
                         xlxz,
                         CASE WHEN SFNW = '1' THEN '3'ELSE '2' END CNW
                  FROM pwyw_arch.t_sb_zwyc_xl
                  WHERE sfnw is NOT null
                  union all
                  SELECT OBJ_ID,
                         b.sjdwid SSDS,
                         xlxz,
                         CASE WHEN SFNW = '1' THEN '3'ELSE '2' END CNW
                  FROM pwyw_arch.t_sb_zwyc_xl a JOIN pwyw_arch.st_pms_yx_dw b ON (a.SSDS = b.pms_dwid)
                  WHERE sfnw is NOT null) t
                LEFT JOIN pwyw_arch.ST_PMS_YX_DW t2
                    ON (t.ssds = t2.pms_dwid)
                LEFT JOIN pwyw_arch.st_bdz_xl t3
                    ON (t.OBJ_ID = t3.pms_xlbs)
                LEFT JOIN (select * from pwyw_arch.e_dms_xl_curve where dt >= '${dateFrom}' AND dt < '${dateTo}') t4
                    ON (t.obj_id = t4.xlid)"""
    val joinData = hc.sql(sql)
    //    val joinData = querySSJoinData(hc, statDate, dateFrom, dateTo, DateFormatUtil.STAT_CYCLE_DAY).distinct()

    val dfGroup = joinData.groupBy("DWBM", "DWJB", "XLXZ", "CNW", "TJRQ")
    val df = dfGroup.agg(
      countDistinct("OBJ_ID") as ("PMSKXSL"),
      countDistinct("YX_XLBS") as ("YCFGKXSL"),
      countDistinct("XLID") as ("CJKXSL"),
      countDistinct(expr("case when UA is not null then XLID end")) as ("AXDYQXSCDS"),
      countDistinct(expr("case when UB is not null then XLID end")) as ("BXDYQXSCDS"),
      countDistinct(expr("case when UC is not null then XLID end")) as ("CXDYQXSCDS"),
      countDistinct(expr("case when IA is not null then XLID end")) as ("AXDLQXSCDS"),
      countDistinct(expr("case when IB is not null then XLID end")) as ("BXDLQXSCDS"),
      countDistinct(expr("case when IC is not null then XLID end")) as ("CXDLQXSCDS"),
      countDistinct(expr("case when P is not null then XLID end")) as ("YGZGLQXSCDS"),
      countDistinct(expr("case when Q is not null then XLID end")) as ("WGZGLQXSCDS"),
      countDistinct(expr("case when F is not null then XLID end")) as ("GLYSQXSCDS"))
    val res =
      df.select($"DWBM", $"DWJB", $"XLXZ", $"CNW", $"PMSKXSL", $"YCFGKXSL", $"CJKXSL",
        $"TJRQ",
        expr(s"${ycds} * YCFGKXSL AXDYQXYCDS"),
        $"AXDYQXSCDS",
        expr(s"${ycds} * YCFGKXSL BXDYQXYCDS"),
        $"BXDYQXSCDS",
        expr(s"${ycds} * YCFGKXSL CXDYQXYCDS"),
        $"CXDYQXSCDS",
        expr(s"${ycds} * YCFGKXSL AXDLQXYCDS"),
        $"AXDLQXSCDS",
        expr(s"${ycds} * YCFGKXSL BXDLQXYCDS"),
        $"BXDLQXSCDS",
        expr(s"${ycds} * YCFGKXSL CXDLQXYCDS"),
        $"CXDLQXSCDS",
        expr(s"${ycds} * YCFGKXSL YGZGLQXYCDS"),
        $"YGZGLQXSCDS",
        expr(s"${ycds} * YCFGKXSL WGZGLQXYCDS"),
        $"WGZGLQXSCDS",
        expr(s"${ycds} * YCFGKXSL GLYSQXYCDS"),
        $"GLYSQXSCDS")
    res
  }

  def statBZGatherSuccRateDay: DataFrame = {
    import hc.implicits._
    val dateFrom = DateFormatUtil.dateFrom(statDate, DateFormatUtil.STAT_CYCLE_DAY)
    val dateTo = DateFormatUtil.dateTo(dateFrom, DateFormatUtil.STAT_CYCLE_DAY)

    val ycds = DateFormatUtil.statDays(dateFrom, dateTo, DateFormatUtil.STAT_CYCLE_DAY) * 96l

    val tjrq = s"'${statDate}' TJRQ\n"

    val sql = s"""SELECT t.OBJ_ID,
                         t.WHBZ DWBM,
                         t2.PMS_DWCJ DWJB,
                         t.CNW,
                         t.XLXZ,
                         t3.YX_XLBS,
                         t4.XLID,
                         t4.SJSJ DATA_TIME,
                         t4.UAB UA,
                         t4.UBC UB,
                         t4.UCA UC,
                         t4.IA,
                         t4.IB,
                         t4.IC,
                         t4.P,
                         t4.Q,
                         NULL F,
                         ${tjrq}
                FROM 
                  (SELECT OBJ_ID,WHBZ,xlxz,CASE WHEN SFNW = '1' THEN '3'ELSE '2' END CNW
                  FROM pwyw_arch.T_SB_ZWYC_XL
                  WHERE SFNW is NOT null
                  UNION all
                  SELECT OBJ_ID,b.sjdwid WHBZ,xlxz,CASE WHEN SFNW = '1' THEN '3'ELSE '2' END CNW
                  FROM pwyw_arch.T_SB_ZWYC_XL a JOIN pwyw_arch.ST_PMS_YX_DW b ON (a.WHBZ = b.pms_dwid)
                  WHERE SFNW is NOT null) t
                LEFT JOIN pwyw_arch.ST_PMS_YX_DW t2
                    ON (t.WHBZ = t2.PMS_DWID)
                LEFT JOIN pwyw_arch.ST_BDZ_XL t3
                    ON (t.OBJ_ID = t3.PMS_XLBS)
                LEFT JOIN (select * from pwyw_arch.E_DMS_XL_CURVE where dt >= '${dateFrom}' AND dt < '${dateTo}') t4
                    ON (t.obj_id = t4.xlid)"""

    val joinData = hc.sql(sql)
    //    val joinData = querySSJoinData(hc, statDate, dateFrom, dateTo, DateFormatUtil.STAT_CYCLE_DAY).distinct()

    val dfGroup = joinData.groupBy("DWBM", "DWJB", "XLXZ", "CNW", "TJRQ")
    val df = dfGroup.agg(
      countDistinct("OBJ_ID") as ("PMSKXSL"),
      countDistinct("YX_XLBS") as ("YCFGKXSL"),
      countDistinct("XLID") as ("CJKXSL"),
      countDistinct(expr("case when UA is not null then XLID end")) as ("AXDYQXSCDS"),
      countDistinct(expr("case when UB is not null then XLID end")) as ("BXDYQXSCDS"),
      countDistinct(expr("case when UC is not null then XLID end")) as ("CXDYQXSCDS"),
      countDistinct(expr("case when IA is not null then XLID end")) as ("AXDLQXSCDS"),
      countDistinct(expr("case when IB is not null then XLID end")) as ("BXDLQXSCDS"),
      countDistinct(expr("case when IC is not null then XLID end")) as ("CXDLQXSCDS"),
      countDistinct(expr("case when P is not null then XLID end")) as ("YGZGLQXSCDS"),
      countDistinct(expr("case when Q is not null then XLID end")) as ("WGZGLQXSCDS"),
      countDistinct(expr("case when F is not null then XLID end")) as ("GLYSQXSCDS"))
    val res = df.select($"DWBM", $"DWJB", $"XLXZ", $"CNW", $"PMSKXSL", $"YCFGKXSL", $"CJKXSL",
      $"TJRQ",
      expr(s"${ycds} * YCFGKXSL AXDYQXYCDS"),
      $"AXDYQXSCDS",
      expr(s"${ycds} * YCFGKXSL BXDYQXYCDS"),
      $"BXDYQXSCDS",
      expr(s"${ycds} * YCFGKXSL CXDYQXYCDS"),
      $"CXDYQXSCDS",
      expr(s"${ycds} * YCFGKXSL AXDLQXYCDS"),
      $"AXDLQXSCDS",
      expr(s"${ycds} * YCFGKXSL BXDLQXYCDS"),
      $"BXDLQXSCDS",
      expr(s"${ycds} * YCFGKXSL CXDLQXYCDS"),
      $"CXDLQXSCDS",
      expr(s"${ycds} * YCFGKXSL YGZGLQXYCDS"),
      $"YGZGLQXSCDS",
      expr(s"${ycds} * YCFGKXSL WGZGLQXYCDS"),
      $"WGZGLQXSCDS",
      expr(s"${ycds} * YCFGKXSL GLYSQXYCDS"),
      $"GLYSQXSCDS")
    res
  }

  def statExceptDay: DataFrame = {
    import hc.implicits._
    val dateFrom = DateFormatUtil.dateFrom(statDate, DateFormatUtil.STAT_CYCLE_DAY)
    val dateTo = DateFormatUtil.dateTo(dateFrom, DateFormatUtil.STAT_CYCLE_DAY)
    val tjrq = s"'${statDate}' TJRQ,\n"

    val sql = s"""SELECT a.XLID,
                         a.XLMC,
                         a.YXBH,
                         a.DYDJ,
                         a.SSDS,
                         a.YWDW,
                         a.YWDWMC,
                         a.DWBM,
                         a.DWMC,
                         a.DWJB,
                         a.SJDWBM,
                         a.SJDWMC,
                         a.BZID,
                         a.SSDZ,
                         a.CNW,
                         b.ZXID,
                         b.SSZX,
                         b.XLXZ,
                         b.ZYCD,
                         b.QDDZ,
                         b.ZDDZ,
                         b.TYRQ,
                         b.ZDYXDL,
                         b.JJDL,
                         b.YXFHXE,
                         b.EDSSGL,
                         b.XLZCD,
                         b.JKXLCD,
                         b.DLXLCD,
                         b.JKJXFS,
                         ${tjrq}
                         a.GJBM,
                         a.SJDJ,
                         a.DQGJFSSC,
                         a.DQGJFSCS,
                         a.WGQBC,
                         a.WGGBC,
                         DYYCGDDS,
                         DYYCGGDS
                FROM 
                  (SELECT XLID,
                         XLMC,
                         YXBH,
                         DYDJ,
                         SSDS,
                         YWDW,
                         YWDWMC,
                         DWBM,
                         DWMC,
                         DWJB,
                         SJDWBM,
                         SJDWMC,
                         BZID,
                         SSDZ,
                         CNW,
                         GJBM,
                         SJDJ,
                         DQGJFSSC,
                         DQGJFSCS,
                         WGQBC,
                         WGGBC,
                         DYYCGDDS,
                         DYYCGGDS
                  FROM pwyw.PWYW_KXYDYC
                  WHERE dt >= '${dateFrom}'
                          AND dt < '${dateTo}' and dqgjfssj >= '${dateFrom}' and dqgjfssj < '${dateTo}') a
                JOIN pwyw_arch.T_SB_ZWYC_XL b
                    ON (a.XLID = b.OBJ_ID)"""
    val data = hc.sql(sql).distinct()
    val dfGroup = data.groupBy("XLID", "XLMC", "YXBH", "DYDJ", "SSDS", "YWDW", "YWDWMC", "DWBM", "DWMC", "DWJB", "SJDWBM", "SJDWMC", "BZID", "SSDZ", "CNW", "ZXID", "SSZX", "XLXZ", "ZYCD", "QDDZ", "ZDDZ", "TYRQ", "ZDYXDL", "JJDL", "YXFHXE", "EDSSGL", "XLZCD", "JKXLCD", "DLXLCD", "JKJXFS", "TJRQ")
    val df = dfGroup.agg(
      min(expr("case when GJBM = '00110' then SJDJ end")) as ("A00110"),
      sum(expr("case when GJBM = '00110' then DQGJFSSC end")) as ("A00110_SJ"),
      sum(expr("case when GJBM = '00110' then DQGJFSCS end")) as ("A00110_CS"),
      min(expr("case when GJBM = '00111' then SJDJ end")) as ("A00111"),
      sum(expr("case when GJBM = '00111' then DQGJFSSC end")) as ("A00111_SJ"),
      sum(expr("case when GJBM = '00111' then DQGJFSCS end")) as ("A00111_CS"),
      min(expr("case when GJBM = '00115' then SJDJ end")) as ("A00115"),
      sum(expr("case when GJBM = '00115' then DQGJFSSC end")) as ("A00115_SJ"),
      sum(expr("case when GJBM = '00115' then DQGJFSCS end")) as ("A00115_CS"),
      min(expr("case when GJBM = '00116' then SJDJ end")) as ("A00116"),
      sum(expr("case when GJBM = '00116' then DQGJFSSC end")) as ("A00116_SJ"),
      sum(expr("case when GJBM = '00116' then DQGJFSCS end")) as ("A00116_CS"),
      min(expr("case when GJBM = '00118' then SJDJ end")) as ("A00118"),
      sum(expr("case when GJBM = '00118' then DQGJFSSC end")) as ("A00118_SJ"),
      sum(expr("case when GJBM = '00118' then DQGJFSCS end")) as ("A00118_CS"),
      min(expr("case when GJBM = '00112' then SJDJ end")) as ("A00112"),
      sum(expr("case when GJBM = '00112' then DQGJFSSC end")) as ("A00112_SJ"),
      sum(expr("case when GJBM = '00112' then DQGJFSCS end")) as ("A00112_CS"),
      min(expr("case when GJBM = '00130' then SJDJ end")) as ("A00130"),
      sum(expr("case when GJBM = '00130' then DQGJFSSC end")) as ("A00130_SJ"),
      sum(expr("case when GJBM = '00130' then DQGJFSCS end")) as ("A00130_CS"),
      min(expr("case when GJBM = '00131' then SJDJ end")) as ("A00131"),
      sum(expr("case when GJBM = '00131' then DQGJFSSC end")) as ("A00131_SJ"),
      sum(expr("case when GJBM = '00131' then DQGJFSCS end")) as ("A00131_CS"),
      min(expr("case when GJBM = '00132' then SJDJ end")) as ("A00132"),
      sum(expr("case when GJBM = '00132' then DQGJFSSC end")) as ("A00132_SJ"),
      sum(expr("case when GJBM = '00132' then DQGJFSCS end")) as ("A00132_CS"),
      min(expr("case when GJBM = '00133' then SJDJ end")) as ("A00133"),
      sum(expr("case when GJBM = '00133' then DQGJFSSC end")) as ("A00133_SJ"),
      sum(expr("case when GJBM = '00133' then DQGJFSCS end")) as ("A00133_CS"),
      min(expr("case when GJBM = '00134' then SJDJ end")) as ("A00134"),
      sum(expr("case when GJBM = '00134' then DQGJFSSC end")) as ("A00134_SJ"),
      sum(expr("case when GJBM = '00134' then DQGJFSCS end")) as ("A00134_CS"),
      min(expr("case when GJBM = '00135' then SJDJ end")) as ("A00135"),
      sum(expr("case when GJBM = '00135' then DQGJFSSC end")) as ("A00135_SJ"),
      sum(expr("case when GJBM = '00135' then DQGJFSCS end")) as ("A00135_CS"),
      min(expr("case when GJBM = '00136' then SJDJ end")) as ("A00136"),
      sum(expr("case when GJBM = '00136' then DQGJFSSC end")) as ("A00136_SJ"),
      sum(expr("case when GJBM = '00136' then DQGJFSCS end")) as ("A00136_CS"),
      min(expr("case when GJBM = '00137' then SJDJ end")) as ("A00137"),
      sum(expr("case when GJBM = '00137' then DQGJFSSC end")) as ("A00137_SJ"),
      sum(expr("case when GJBM = '00137' then DQGJFSCS end")) as ("A00137_CS"),
      min(expr("case when GJBM = '00138' then SJDJ end")) as ("A00138"),
      sum(expr("case when GJBM = '00138' then DQGJFSSC end")) as ("A00138_SJ"),
      sum(expr("case when GJBM = '00138' then DQGJFSCS end")) as ("A00138_CS"),
      min(expr("case when GJBM = '00139' then SJDJ end")) as ("A00139"),
      sum(expr("case when GJBM = '00139' then DQGJFSSC end")) as ("A00139_SJ"),
      sum(expr("case when GJBM = '00139' then DQGJFSCS end")) as ("A00139_CS"),
      min(expr("case when GJBM = '0013A' then SJDJ end")) as ("A0013A"),
      sum(expr("case when GJBM = '0013A' then DQGJFSSC end")) as ("A0013A_SJ"),
      sum(expr("case when GJBM = '0013A' then DQGJFSCS end")) as ("A0013A_CS"),
      min(expr("case when GJBM in ('00130','00131','00132') then SJDJ end")) as ("A0013R"),
      sum(expr("case when GJBM in ('00130','00131','00132') then DQGJFSSC end")) as ("A0013R_SJ"),
      sum(expr("case when GJBM in ('00130','00131','00132') then DQGJFSCS end")) as ("A0013R_CS"),
      min(expr("case when GJBM in ('00133','00134','00135') then SJDJ end")) as ("A0013S"),
      sum(expr("case when GJBM in ('00133','00134','00135') then DQGJFSSC end")) as ("A0013S_SJ"),
      sum(expr("case when GJBM in ('00133','00134','00135') then DQGJFSCS end")) as ("A0013S_CS"),
      min(expr("case when GJBM in ('00136','00137','00138') then SJDJ end")) as ("A0013T"),
      sum(expr("case when GJBM in ('00136','00137','00138') then DQGJFSSC end")) as ("A0013T_SJ"),
      sum(expr("case when GJBM in ('00136','00137','00138') then DQGJFSCS end")) as ("A0013T_CS"),

//      max(expr("null")) as ("A00112_XB"),
//      max(expr("null")) as ("A00112_ZDBPHD"),
//      max(expr("null")) as ("A00112_ZDBPHD_SJ"),
//      max(expr("null")) as ("A00118_XB"),
//      max(expr("null")) as ("A00118_ZDBPHD"),
//      max(expr("null")) as ("A00118_ZDBPHD_SJ"),

      max("WGQBC") as ("WGQBC"),
      max("WGGBC") as ("WGGBC"),
      sum("DYYCGDDS") as ("DYYCGDDS"),
      sum("DYYCGGDS") as ("DYYCGGDS"))
    df
  }

  def statSSGatherSuccRateSeasonAndMore: DataFrame = {
    import hc.implicits._
    val dateFrom = DateFormatUtil.dateFrom(statDate, statCycle)
    val dateTo = DateFormatUtil.dateTo(dateFrom, statCycle)

    val ycds = DateFormatUtil.statDays(dateFrom, dateTo, statCycle) * 96l

    val weekDateTo = DateFormatUtil.dateDay(dateTo, 1)
    val tjrq = if (DateFormatUtil.STAT_CYCLE_WEEK.equals(statCycle)) s"'${dateFrom}' TJKSRQ, '${weekDateTo}' TJJSRQ\n"
    else s"'${statDate}' TJRQ\n"

    val sql = s"""SELECT t.OBJ_ID,
                         t.SSDS DWBM,
                         t2.PMS_DWCJ DWJB,
                         t.CNW,
                         t.XLXZ,
                         t3.YX_XLBS,
                         t4.XLID,
                         t4.SJSJ DATA_TIME,
                         t4.UAB UA,
                         t4.UBC UB,
                         t4.UCA UC,
                         t4.IA,
                         t4.IB,
                         t4.IC,
                         t4.P,
                         t4.Q,
                         NULL F,
                         ${tjrq}
                FROM 
                  (SELECT OBJ_ID,
                         SSDS,
                         xlxz,
                         CASE WHEN SFNW = '1' THEN '3'ELSE '2' END CNW
                  FROM pwyw_arch.t_sb_zwyc_xl
                  WHERE sfnw is NOT null
                  union all
                  SELECT OBJ_ID,
                         b.sjdwid SSDS,
                         xlxz,
                         CASE WHEN SFNW = '1' THEN '3'ELSE '2' END CNW
                  FROM pwyw_arch.t_sb_zwyc_xl a JOIN pwyw_arch.st_pms_yx_dw b ON (a.SSDS = b.pms_dwid)
                  WHERE sfnw is NOT null) t
                LEFT JOIN pwyw_arch.ST_PMS_YX_DW t2
                    ON (t.ssds = t2.pms_dwid)
                LEFT JOIN pwyw_arch.st_bdz_xl t3
                    ON (t.OBJ_ID = t3.pms_xlbs)
                LEFT JOIN (select * from pwyw_arch.e_dms_xl_curve where dt >= '${dateFrom}' AND dt < '${dateTo}') t4
                    ON (t.obj_id = t4.xlid)"""
    val joinData = hc.sql(sql)
    //    val joinData = querySSJoinData(hc, statDate, dateFrom, dateTo, statCycle).distinct()

    val dfGroup = if (DateFormatUtil.STAT_CYCLE_WEEK.equals(statCycle)) joinData.groupBy("DWBM", "DWJB", "XLXZ", "CNW", "TJKSRQ", "TJJSRQ")
    else joinData.groupBy("DWBM", "DWJB", "XLXZ", "CNW", "TJRQ")
    val df = dfGroup.agg(
      countDistinct("OBJ_ID") as ("PMSKXSL"),
      countDistinct("YX_XLBS") as ("YCFGKXSL"),
      countDistinct("XLID") as ("CJKXSL"),
      countDistinct(expr("case when UA is not null then XLID end")) as ("AXDYQXSCDS"),
      countDistinct(expr("case when UB is not null then XLID end")) as ("BXDYQXSCDS"),
      countDistinct(expr("case when UC is not null then XLID end")) as ("CXDYQXSCDS"),
      countDistinct(expr("case when IA is not null then XLID end")) as ("AXDLQXSCDS"),
      countDistinct(expr("case when IB is not null then XLID end")) as ("BXDLQXSCDS"),
      countDistinct(expr("case when IC is not null then XLID end")) as ("CXDLQXSCDS"),
      countDistinct(expr("case when P is not null then XLID end")) as ("YGZGLQXSCDS"),
      countDistinct(expr("case when Q is not null then XLID end")) as ("WGZGLQXSCDS"),
      countDistinct(expr("case when F is not null then XLID end")) as ("GLYSQXSCDS"))
    val res = if (DateFormatUtil.STAT_CYCLE_WEEK.equals(statCycle))
      df.select($"DWBM", $"DWJB", $"XLXZ", $"CNW", $"PMSKXSL", $"YCFGKXSL", $"CJKXSL",
        $"TJKSRQ", $"TJJSRQ",
        expr(s"${ycds} * YCFGKXSL AXDYQXYCDS"),
        $"AXDYQXSCDS",
        expr(s"${ycds} * YCFGKXSL BXDYQXYCDS"),
        $"BXDYQXSCDS",
        expr(s"${ycds} * YCFGKXSL CXDYQXYCDS"),
        $"CXDYQXSCDS",
        expr(s"${ycds} * YCFGKXSL AXDLQXYCDS"),
        $"AXDLQXSCDS",
        expr(s"${ycds} * YCFGKXSL BXDLQXYCDS"),
        $"BXDLQXSCDS",
        expr(s"${ycds} * YCFGKXSL CXDLQXYCDS"),
        $"CXDLQXSCDS",
        expr(s"${ycds} * YCFGKXSL YGZGLQXYCDS"),
        $"YGZGLQXSCDS",
        expr(s"${ycds} * YCFGKXSL WGZGLQXYCDS"),
        $"WGZGLQXSCDS",
        expr(s"${ycds} * YCFGKXSL GLYSQXYCDS"),
        $"GLYSQXSCDS")
    else
      df.select($"DWBM", $"DWJB", $"XLXZ", $"CNW", $"PMSKXSL", $"YCFGKXSL", $"CJKXSL",
        $"TJRQ",
        expr(s"${ycds} * YCFGKXSL AXDYQXYCDS"),
        $"AXDYQXSCDS",
        expr(s"${ycds} * YCFGKXSL BXDYQXYCDS"),
        $"BXDYQXSCDS",
        expr(s"${ycds} * YCFGKXSL CXDYQXYCDS"),
        $"CXDYQXSCDS",
        expr(s"${ycds} * YCFGKXSL AXDLQXYCDS"),
        $"AXDLQXSCDS",
        expr(s"${ycds} * YCFGKXSL BXDLQXYCDS"),
        $"BXDLQXSCDS",
        expr(s"${ycds} * YCFGKXSL CXDLQXYCDS"),
        $"CXDLQXSCDS",
        expr(s"${ycds} * YCFGKXSL YGZGLQXYCDS"),
        $"YGZGLQXSCDS",
        expr(s"${ycds} * YCFGKXSL WGZGLQXYCDS"),
        $"WGZGLQXSCDS",
        expr(s"${ycds} * YCFGKXSL GLYSQXYCDS"),
        $"GLYSQXSCDS")
    res
  }

  def statBZGatherSuccRateSeasonAndMore: DataFrame = {
    import hc.implicits._
    val dateFrom = DateFormatUtil.dateFrom(statDate, statCycle)
    val dateTo = DateFormatUtil.dateTo(dateFrom, statCycle)

    val ycds = DateFormatUtil.statDays(dateFrom, dateTo, statCycle) * 96l

    val weekDateTo = DateFormatUtil.dateDay(dateTo, 1)
    val tjrq = if (DateFormatUtil.STAT_CYCLE_WEEK.equals(statCycle)) s"'${dateFrom}' TJKSRQ, '${weekDateTo}' TJJSRQ\n"
    else s"'${statDate}' TJRQ\n"

    val sql = s"""SELECT t.OBJ_ID,
                         t.WHBZ DWBM,
                         t2.PMS_DWCJ DWJB,
                         t.CNW,
                         t.XLXZ,
                         t3.YX_XLBS,
                         t4.XLID,
                         t4.SJSJ DATA_TIME,
                         t4.UAB UA,
                         t4.UBC UB,
                         t4.UCA UC,
                         t4.IA,
                         t4.IB,
                         t4.IC,
                         t4.P,
                         t4.Q,
                         NULL F,
                         ${tjrq}
                FROM 
                  (SELECT OBJ_ID,WHBZ,xlxz,CASE WHEN SFNW = '1' THEN '3'ELSE '2' END CNW
                  FROM pwyw_arch.T_SB_ZWYC_XL
                  WHERE SFNW is NOT null
                  UNION all
                  SELECT OBJ_ID,b.sjdwid WHBZ,xlxz,CASE WHEN SFNW = '1' THEN '3'ELSE '2' END CNW
                  FROM pwyw_arch.T_SB_ZWYC_XL a JOIN pwyw_arch.ST_PMS_YX_DW b ON (a.WHBZ = b.pms_dwid)
                  WHERE SFNW is NOT null) t
                LEFT JOIN pwyw_arch.ST_PMS_YX_DW t2
                    ON (t.WHBZ = t2.PMS_DWID)
                LEFT JOIN pwyw_arch.ST_BDZ_XL t3
                    ON (t.OBJ_ID = t3.PMS_XLBS)
                LEFT JOIN (select * from pwyw_arch.E_DMS_XL_CURVE where dt >= '${dateFrom}' AND dt < '${dateTo}') t4
                    ON (t.obj_id = t4.xlid)"""

    val joinData = hc.sql(sql)
    //    val joinData = querySSJoinData(hc, statDate, dateFrom, dateTo, statCycle).distinct()

    val dfGroup = if (DateFormatUtil.STAT_CYCLE_WEEK.equals(statCycle)) joinData.groupBy("DWBM", "DWJB", "XLXZ", "CNW", "TJKSRQ", "TJJSRQ")
    else joinData.groupBy("DWBM", "DWJB", "XLXZ", "CNW", "TJRQ")
    val df = dfGroup.agg(
      countDistinct("OBJ_ID") as ("PMSKXSL"),
      countDistinct("YX_XLBS") as ("YCFGKXSL"),
      countDistinct("XLID") as ("CJKXSL"),
      countDistinct(expr("case when UA is not null then XLID end")) as ("AXDYQXSCDS"),
      countDistinct(expr("case when UB is not null then XLID end")) as ("BXDYQXSCDS"),
      countDistinct(expr("case when UC is not null then XLID end")) as ("CXDYQXSCDS"),
      countDistinct(expr("case when IA is not null then XLID end")) as ("AXDLQXSCDS"),
      countDistinct(expr("case when IB is not null then XLID end")) as ("BXDLQXSCDS"),
      countDistinct(expr("case when IC is not null then XLID end")) as ("CXDLQXSCDS"),
      countDistinct(expr("case when P is not null then XLID end")) as ("YGZGLQXSCDS"),
      countDistinct(expr("case when Q is not null then XLID end")) as ("WGZGLQXSCDS"),
      countDistinct(expr("case when F is not null then XLID end")) as ("GLYSQXSCDS"))
    val res = if (DateFormatUtil.STAT_CYCLE_WEEK.equals(statCycle))
      df.select($"DWBM", $"DWJB", $"XLXZ", $"CNW", $"PMSKXSL", $"YCFGKXSL", $"CJKXSL",
        $"TJKSRQ", $"TJJSRQ",
        expr(s"${ycds} * YCFGKXSL AXDYQXYCDS"),
        $"AXDYQXSCDS",
        expr(s"${ycds} * YCFGKXSL BXDYQXYCDS"),
        $"BXDYQXSCDS",
        expr(s"${ycds} * YCFGKXSL CXDYQXYCDS"),
        $"CXDYQXSCDS",
        expr(s"${ycds} * YCFGKXSL AXDLQXYCDS"),
        $"AXDLQXSCDS",
        expr(s"${ycds} * YCFGKXSL BXDLQXYCDS"),
        $"BXDLQXSCDS",
        expr(s"${ycds} * YCFGKXSL CXDLQXYCDS"),
        $"CXDLQXSCDS",
        expr(s"${ycds} * YCFGKXSL YGZGLQXYCDS"),
        $"YGZGLQXSCDS",
        expr(s"${ycds} * YCFGKXSL WGZGLQXYCDS"),
        $"WGZGLQXSCDS",
        expr(s"${ycds} * YCFGKXSL GLYSQXYCDS"),
        $"GLYSQXSCDS")
    else
      df.select($"DWBM", $"DWJB", $"XLXZ", $"CNW", $"PMSKXSL", $"YCFGKXSL", $"CJKXSL",
        $"TJRQ",
        expr(s"${ycds} * YCFGKXSL AXDYQXYCDS"),
        $"AXDYQXSCDS",
        expr(s"${ycds} * YCFGKXSL BXDYQXYCDS"),
        $"BXDYQXSCDS",
        expr(s"${ycds} * YCFGKXSL CXDYQXYCDS"),
        $"CXDYQXSCDS",
        expr(s"${ycds} * YCFGKXSL AXDLQXYCDS"),
        $"AXDLQXSCDS",
        expr(s"${ycds} * YCFGKXSL BXDLQXYCDS"),
        $"BXDLQXSCDS",
        expr(s"${ycds} * YCFGKXSL CXDLQXYCDS"),
        $"CXDLQXSCDS",
        expr(s"${ycds} * YCFGKXSL YGZGLQXYCDS"),
        $"YGZGLQXSCDS",
        expr(s"${ycds} * YCFGKXSL WGZGLQXYCDS"),
        $"WGZGLQXSCDS",
        expr(s"${ycds} * YCFGKXSL GLYSQXYCDS"),
        $"GLYSQXSCDS")
    res
  }

  def statExceptSeasonAndMore: DataFrame = {
    import hc.implicits._
    val dateFrom = DateFormatUtil.dateFrom(statDate, statCycle)
    val dateTo = DateFormatUtil.dateTo(dateFrom, statCycle)
    val weekDateTo = DateFormatUtil.dateDay(dateTo, 1)
    val tjrq = if (DateFormatUtil.STAT_CYCLE_WEEK.equals(statCycle)) s"'${dateFrom}' TJKSRQ, '${weekDateTo}' TJJSRQ\n"
    else s"'${statDate}' TJRQ\n"

    val sql1 = s"""select XLID,
       XLMC,
       YXBH,
       DYDJ,
       SSDS,
       YWDW,
       YWDWMC,
       DWBM,
       DWMC,
       DWJB,
       SJDWBM,
       SJDWMC,
       BZID,
       SSDZ,
       CNW,
       ZXID,
       SSZX,
       XLXZ,
       ZYCD,
       QDDZ,
       ZDDZ,
       TYRQ,
       JSFS,
       ZDYXDL,
       JJDL,
       YXFHXE,
       EDSSGL,
       XLZCD,
       JKXLCD,
       DLXLCD,
       JKJXFS,
       ${tjrq}
  from pwyw_kxyctj_y
 where tjrq >= '${dateFrom}'
   and tjrq < '${dateTo}'
 group by XLID,
       XLMC,
       YXBH,
       DYDJ,
       SSDS,
       YWDW,
       YWDWMC,
       DWBM,
       DWMC,
       DWJB,
       SJDWBM,
       SJDWMC,
       BZID,
       SSDZ,
       CNW,
       ZXID,
       SSZX,
       XLXZ,
       ZYCD,
       QDDZ,
       ZDDZ,
       TYRQ,
       JSFS,
       ZDYXDL,
       JJDL,
       YXFHXE,
       EDSSGL,
       XLZCD,
       JKXLCD,
       DLXLCD,
       JKJXFS"""

    val arch = hc.read.jdbc(JdbcConnUtil.url, s"(${sql1})", JdbcConnUtil.connProp)

    val sql2 = s"""select XLID,
       A00115,
       A00115_SJ,
       A00115_CS,
       A00115_TS,
       A00139,
       A00139_SJ,
       A00139_CS,
       A00139_TS,
       A0013A,
       A0013A_SJ,
       A0013A_CS,
       A0013A_TS,
       A00110,
       A00110_SJ,
       A00110_CS,
       A00110_TS,
       A00111,
       A00111_SJ,
       A00111_CS,
       A00111_TS,
       A00118,
       A00118_SJ,
       A00118_CS,
       A00118_TS,
       A00112,
       A00112_SJ,
       A00112_CS,
       A00112_TS,
       A00130,
       A00130_SJ,
       A00130_CS,
       A00130_TS,
       A00131,
       A00131_SJ,
       A00131_CS,
       A00131_TS,
       A00132,
       A00132_SJ,
       A00132_CS,
       A00132_TS,
       A00133,
       A00133_SJ,
       A00133_CS,
       A00133_TS,
       A00134,
       A00134_SJ,
       A00134_CS,
       A00134_TS,
       A00135,
       A00135_SJ,
       A00135_CS,
       A00135_TS,
       A00136,
       A00136_SJ,
       A00136_CS,
       A00136_TS,
       A00137,
       A00137_SJ,
       A00137_CS,
       A00137_TS,
       A00138,
       A00138_SJ,
       A00138_CS,
       A00138_TS,
       A00116,
       A00116_SJ,
       A00116_CS,
       A00116_TS,
       REA_LOW_CS WGQB,
       REA_OVER_CS WGGB,
       DYYCGDDS,
       DYYCGGDS,
       A0013S,
       A0013S_SJ,
       A0013S_CS,
       A0013S_TS,
       A0013T,
       A0013T_SJ,
       A0013T_CS,
       A0013T_TS,
       A0013R,
       A0013R_SJ,
       A0013R_CS,
       A0013R_TS
  from pwyw_kxyctj_y
 where tjrq >= '${dateFrom}'
   and tjrq < '${dateTo}'"""

    val data = hc.read.jdbc(JdbcConnUtil.url, s"(${sql2})", JdbcConnUtil.connProp)
      .select($"XLID",
        $"A00115",
        expr("cast(A00115_SJ as double)") as "A00115_SJ",
        expr("cast(A00115_CS as int)") as "A00115_CS",
        expr("cast(A00115_TS as int)") as "A00115_TS",
        $"A00139",
        expr("cast(A00139_SJ as double)") as "A00139_SJ",
        expr("cast(A00139_CS as int)") as "A00139_CS",
        expr("cast(A00139_TS as int)") as "A00139_TS",
        $"A0013A",
        expr("cast(A0013A_SJ as double)") as "A0013A_SJ",
        expr("cast(A0013A_CS as int)") as "A0013A_CS",
        expr("cast(A0013A_TS as int)") as "A0013A_TS",
        $"A00110",
        expr("cast(A00110_SJ as double)") as "A00110_SJ",
        expr("cast(A00110_CS as int)") as "A00110_CS",
        expr("cast(A00110_TS as int)") as "A00110_TS",
        $"A00111",
        expr("cast(A00111_SJ as double)") as "A00111_SJ",
        expr("cast(A00111_CS as int)") as "A00111_CS",
        expr("cast(A00111_TS as int)") as "A00111_TS",
        $"A00118",
        expr("cast(A00118_SJ as double)") as "A00118_SJ",
        expr("cast(A00118_CS as int)") as "A00118_CS",
        expr("cast(A00118_TS as int)") as "A00118_TS",
        $"A00112",
        expr("cast(A00112_SJ as double)") as "A00112_SJ",
        expr("cast(A00112_CS as int)") as "A00112_CS",
        expr("cast(A00112_TS as int)") as "A00112_TS",
        $"A00130",
        expr("cast(A00130_SJ as double)") as "A00130_SJ",
        expr("cast(A00130_CS as int)") as "A00130_CS",
        expr("cast(A00130_TS as int)") as "A00130_TS",
        $"A00131",
        expr("cast(A00131_SJ as double)") as "A00131_SJ",
        expr("cast(A00131_CS as int)") as "A00131_CS",
        expr("cast(A00131_TS as int)") as "A00131_TS",
        $"A00132",
        expr("cast(A00132_SJ as double)") as "A00132_SJ",
        expr("cast(A00132_CS as int)") as "A00132_CS",
        expr("cast(A00132_TS as int)") as "A00132_TS",
        $"A00133",
        expr("cast(A00133_SJ as double)") as "A00133_SJ",
        expr("cast(A00133_CS as int)") as "A00133_CS",
        expr("cast(A00133_TS as int)") as "A00133_TS",
        $"A00134",
        expr("cast(A00134_SJ as double)") as "A00134_SJ",
        expr("cast(A00134_CS as int)") as "A00134_CS",
        expr("cast(A00134_TS as int)") as "A00134_TS",
        $"A00135",
        expr("cast(A00135_SJ as double)") as "A00135_SJ",
        expr("cast(A00135_CS as int)") as "A00135_CS",
        expr("cast(A00135_TS as int)") as "A00135_TS",
        $"A00136",
        expr("cast(A00136_SJ as double)") as "A00136_SJ",
        expr("cast(A00136_CS as int)") as "A00136_CS",
        expr("cast(A00136_TS as int)") as "A00136_TS",
        $"A00137",
        expr("cast(A00137_SJ as double)") as "A00137_SJ",
        expr("cast(A00137_CS as int)") as "A00137_CS",
        expr("cast(A00137_TS as int)") as "A00137_TS",
        $"A00138",
        expr("cast(A00138_SJ as double)") as "A00138_SJ",
        expr("cast(A00138_CS as int)") as "A00138_CS",
        expr("cast(A00138_TS as int)") as "A00138_TS",
        $"A00116",
        expr("cast(A00116_SJ as double)") as "A00116_SJ",
        expr("cast(A00116_CS as int)") as "A00116_CS",
        expr("cast(A00116_TS as int)") as "A00116_TS",
        expr("cast(WGQB as int)") as "WGQB",
        expr("cast(WGGB as int)") as "WGGB",
        expr("cast(DYYCGDDS as int)") as "DYYCGDDS",
        expr("cast(DYYCGGDS as int)") as "DYYCGGDS",
        $"A0013S",
        expr("cast(A0013S_SJ as double)") as "A0013S_SJ",
        expr("cast(A0013S_CS as int)") as "A0013S_CS",
        expr("cast(A0013S_TS as int)") as "A0013S_TS",
        $"A0013T",
        expr("cast(A0013T_SJ as double)") as "A0013T_SJ",
        expr("cast(A0013T_CS as int)") as "A0013T_CS",
        expr("cast(A0013T_TS as int)") as "A0013T_TS",
        $"A0013R",
        expr("cast(A0013R_SJ as double)") as "A0013R_SJ",
        expr("cast(A0013R_CS as int)") as "A0013R_CS",
        expr("cast(A0013R_TS as int)") as "A0013R_TS")

    val df = data.groupBy("XLID").agg(
      min("A00110") as ("A00110"),
      sum("A00110_SJ") as ("A00110_SJ"),
      sum("A00110_CS") as ("A00110_CS"),
      sum("A00110_TS") as ("A00110_TS"),
      min("A00111") as ("A00111"),
      sum("A00111_SJ") as ("A00111_SJ"),
      sum("A00111_CS") as ("A00111_CS"),
      sum("A00111_TS") as ("A00111_TS"),
      min("A00115") as ("A00115"),
      sum("A00115_SJ") as ("A00115_SJ"),
      sum("A00115_CS") as ("A00115_CS"),
      sum("A00115_TS") as ("A00115_TS"),
      min("A00116") as ("A00116"),
      sum("A00116_SJ") as ("A00116_SJ"),
      sum("A00116_CS") as ("A00116_CS"),
      sum("A00116_TS") as ("A00116_TS"),
      min("A00118") as ("A00118"),
      sum("A00118_SJ") as ("A00118_SJ"),
      sum("A00118_CS") as ("A00118_CS"),
      sum("A00118_TS") as ("A00118_TS"),
      min("A00112") as ("A00112"),
      sum("A00112_SJ") as ("A00112_SJ"),
      sum("A00112_CS") as ("A00112_CS"),
      sum("A00112_TS") as ("A00112_TS"),
      min("A00130") as ("A00130"),
      sum("A00130_SJ") as ("A00130_SJ"),
      sum("A00130_CS") as ("A00130_CS"),
      sum("A00130_TS") as ("A00130_TS"),
      min("A00131") as ("A00131"),
      sum("A00131_SJ") as ("A00131_SJ"),
      sum("A00131_CS") as ("A00131_CS"),
      sum("A00131_TS") as ("A00131_TS"),
      min("A00132") as ("A00132"),
      sum("A00132_SJ") as ("A00132_SJ"),
      sum("A00132_CS") as ("A00132_CS"),
      sum("A00132_TS") as ("A00132_TS"),
      min("A00133") as ("A00133"),
      sum("A00133_SJ") as ("A00133_SJ"),
      sum("A00133_CS") as ("A00133_CS"),
      sum("A00133_TS") as ("A00133_TS"),
      min("A00134") as ("A00134"),
      sum("A00134_SJ") as ("A00134_SJ"),
      sum("A00134_CS") as ("A00134_CS"),
      sum("A00134_TS") as ("A00134_TS"),
      min("A00135") as ("A00135"),
      sum("A00135_SJ") as ("A00135_SJ"),
      sum("A00135_CS") as ("A00135_CS"),
      sum("A00135_TS") as ("A00135_TS"),
      min("A00136") as ("A00136"),
      sum("A00136_SJ") as ("A00136_SJ"),
      sum("A00136_CS") as ("A00136_CS"),
      sum("A00136_TS") as ("A00136_TS"),
      min("A00137") as ("A00137"),
      sum("A00137_SJ") as ("A00137_SJ"),
      sum("A00137_CS") as ("A00137_CS"),
      sum("A00137_TS") as ("A00137_TS"),
      min("A00138") as ("A00138"),
      sum("A00138_SJ") as ("A00138_SJ"),
      sum("A00138_CS") as ("A00138_CS"),
      sum("A00138_TS") as ("A00138_TS"),
      min("A00139") as ("A00139"),
      sum("A00139_SJ") as ("A00139_SJ"),
      sum("A00139_CS") as ("A00139_CS"),
      sum("A00139_TS") as ("A00139_TS"),
      min("A0013A") as ("A0013A"),
      sum("A0013A_SJ") as ("A0013A_SJ"),
      sum("A0013A_CS") as ("A0013A_CS"),
      sum("A0013A_TS") as ("A0013A_TS"),
      min("A0013R") as ("A0013R"),
      sum("A0013R_SJ") as ("A0013R_SJ"),
      sum("A0013R_CS") as ("A0013R_CS"),
      sum("A0013R_TS") as ("A0013R_TS"),
      min("A0013S") as ("A0013S"),
      sum("A0013S_SJ") as ("A0013S_SJ"),
      sum("A0013S_CS") as ("A0013S_CS"),
      sum("A0013S_TS") as ("A0013S_TS"),
      min("A0013T") as ("A0013T"),
      sum("A0013T_SJ") as ("A0013T_SJ"),
      sum("A0013T_CS") as ("A0013T_CS"),
      sum("A0013T_TS") as ("A0013T_TS"),

//      max(expr("null")) as ("A00112_XB"),
//      max(expr("null")) as ("A00112_ZDBPHD"),
//      max(expr("null")) as ("A00112_ZDBPHD_SJ"),
//      max(expr("null")) as ("A00118_XB"),
//      max(expr("null")) as ("A00118_ZDBPHD"),
//      max(expr("null")) as ("A00118_ZDBPHD_SJ"),

      max("WGQB") as ("WGQBC"),
      max("WGGB") as ("WGGBC"),
      sum("DYYCGDDS") as ("DYYCGDDS"),
      sum("DYYCGGDS") as ("DYYCGGDS"))

    val res = arch.join(df, Seq("XLID")).distinct()
    res.persist()
    res.printSchema()
    res.show(20)
    res
  }
  def statExceptWeekAndMon: DataFrame = {
    import hc.implicits._
    val dateFrom = DateFormatUtil.dateFrom(statDate, statCycle)
    val dateTo = DateFormatUtil.dateTo(dateFrom, statCycle)
    val weekDateTo = DateFormatUtil.dateDay(dateTo, 1)
    val tjrq = if (DateFormatUtil.STAT_CYCLE_WEEK.equals(statCycle)) s"'${dateFrom}' TJKSRQ, '${weekDateTo}' TJJSRQ\n"
    else s"'${statDate}' TJRQ\n"

    val sql1 = s"""select XLID,
       XLMC,
       YXBH,
       DYDJ,
       SSDS,
       YWDW,
       YWDWMC,
       DWBM,
       DWMC,
       DWJB,
       SJDWBM,
       SJDWMC,
       BZID,
       SSDZ,
       CNW,
       ZXID,
       SSZX,
       XLXZ,
       ZYCD,
       QDDZ,
       ZDDZ,
       TYRQ,
       JSFS,
       ZDYXDL,
       JJDL,
       YXFHXE,
       EDSSGL,
       XLZCD,
       JKXLCD,
       DLXLCD,
       JKJXFS,
       ${tjrq}
  from pwyw_kxyctj_r
 where tjrq >= to_date('${dateFrom}', 'yyyymmdd')
   and tjrq < to_date('${dateTo}', 'yyyymmdd')
 group by XLID,
       XLMC,
       YXBH,
       DYDJ,
       SSDS,
       YWDW,
       YWDWMC,
       DWBM,
       DWMC,
       DWJB,
       SJDWBM,
       SJDWMC,
       BZID,
       SSDZ,
       CNW,
       ZXID,
       SSZX,
       XLXZ,
       ZYCD,
       QDDZ,
       ZDDZ,
       TYRQ,
       JSFS,
       ZDYXDL,
       JJDL,
       YXFHXE,
       EDSSGL,
       XLZCD,
       JKXLCD,
       DLXLCD,
       JKJXFS"""

    val arch = hc.read.jdbc(JdbcConnUtil.url, s"(${sql1})", JdbcConnUtil.connProp)

    val sql2 = s"""select XLID,
       A00115,
       A00115_SJ,
       A00115_CS,
       A00139,
       A00139_SJ,
       A00139_CS,
       A0013A,
       A0013A_SJ,
       A0013A_CS,
       A00110,
       A00110_SJ,
       A00110_CS,
       A00111,
       A00111_SJ,
       A00111_CS,
       A00118,
       A00118_SJ,
       A00118_CS,
       A00112,
       A00112_SJ,
       A00112_CS,
       A00130,
       A00130_SJ,
       A00130_CS,
       A00131,
       A00131_SJ,
       A00131_CS,
       A00132,
       A00132_SJ,
       A00132_CS,
       A00133,
       A00133_SJ,
       A00133_CS,
       A00134,
       A00134_SJ,
       A00134_CS,
       A00135,
       A00135_SJ,
       A00135_CS,
       A00136,
       A00136_SJ,
       A00136_CS,
       A00137,
       A00137_SJ,
       A00137_CS,
       A00138,
       A00138_SJ,
       A00138_CS,
       A00116,
       A00116_SJ,
       A00116_CS,
       WGQB,
       WGGB,
       DYYCGDDS,
       DYYCGGDS,
       A0013S,
       A0013S_SJ,
       A0013S_CS,
       A0013T,
       A0013T_SJ,
       A0013T_CS,
       A0013R,
       A0013R_SJ,
       A0013R_CS
  from pwyw_kxyctj_r
 where tjrq >= to_date('${dateFrom}', 'yyyymmdd')
   and tjrq < to_date('${dateTo}', 'yyyymmdd')"""

    val data = hc.read.jdbc(JdbcConnUtil.url, s"(${sql2})", JdbcConnUtil.connProp)
      .select($"XLID",
        $"A00115",
        expr("cast(A00115_SJ as double)") as "A00115_SJ",
        expr("cast(A00115_CS as int)") as "A00115_CS",
        $"A00139",
        expr("cast(A00139_SJ as double)") as "A00139_SJ",
        expr("cast(A00139_CS as int)") as "A00139_CS",
        $"A0013A",
        expr("cast(A0013A_SJ as double)") as "A0013A_SJ",
        expr("cast(A0013A_CS as int)") as "A0013A_CS",
        $"A00110",
        expr("cast(A00110_SJ as double)") as "A00110_SJ",
        expr("cast(A00110_CS as int)") as "A00110_CS",
        $"A00111",
        expr("cast(A00111_SJ as double)") as "A00111_SJ",
        expr("cast(A00111_CS as int)") as "A00111_CS",
        $"A00118",
        expr("cast(A00118_SJ as double)") as "A00118_SJ",
        expr("cast(A00118_CS as int)") as "A00118_CS",
        //        expr("null") as "A00118_XB",
        //        expr("null") as "A00118_ZDBPHD",
        //        expr("null") as "A00118_ZDBPHD_SJ",
        $"A00112",
        expr("cast(A00112_SJ as double)") as "A00112_SJ",
        expr("cast(A00112_CS as int)") as "A00112_CS",
        //        expr("null") as "A00112_XB",
        //        expr("null") as "A00112_ZDBPHD",
        //        expr("null") as "A00112_ZDBPHD_SJ",
        $"A00130",
        expr("cast(A00130_SJ as double)") as "A00130_SJ",
        expr("cast(A00130_CS as int)") as "A00130_CS",
        $"A00131",
        expr("cast(A00131_SJ as double)") as "A00131_SJ",
        expr("cast(A00131_CS as int)") as "A00131_CS",
        $"A00132",
        expr("cast(A00132_SJ as double)") as "A00132_SJ",
        expr("cast(A00132_CS as int)") as "A00132_CS",
        $"A00133",
        expr("cast(A00133_SJ as double)") as "A00133_SJ",
        expr("cast(A00133_CS as int)") as "A00133_CS",
        $"A00134",
        expr("cast(A00134_SJ as double)") as "A00134_SJ",
        expr("cast(A00134_CS as int)") as "A00134_CS",
        $"A00135",
        expr("cast(A00135_SJ as double)") as "A00135_SJ",
        expr("cast(A00135_CS as int)") as "A00135_CS",
        $"A00136",
        expr("cast(A00136_SJ as double)") as "A00136_SJ",
        expr("cast(A00136_CS as int)") as "A00136_CS",
        $"A00137",
        expr("cast(A00137_SJ as double)") as "A00137_SJ",
        expr("cast(A00137_CS as int)") as "A00137_CS",
        $"A00138",
        expr("cast(A00138_SJ as double)") as "A00138_SJ",
        expr("cast(A00138_CS as int)") as "A00138_CS",
        $"A00116",
        expr("cast(A00116_SJ as double)") as "A00116_SJ",
        expr("cast(A00116_CS as int)") as "A00116_CS",
        expr("cast(WGQB as int)") as "WGQB",
        expr("cast(WGGB as int)") as "WGGB",
        expr("cast(DYYCGDDS as int)") as "DYYCGDDS",
        expr("cast(DYYCGGDS as int)") as "DYYCGGDS",
        $"A0013S",
        expr("cast(A0013S_SJ as double)") as "A0013S_SJ",
        expr("cast(A0013S_CS as int)") as "A0013S_CS",
        $"A0013T",
        expr("cast(A0013T_SJ as double)") as "A0013T_SJ",
        expr("cast(A0013T_CS as int)") as "A0013T_CS",
        $"A0013R",
        expr("cast(A0013R_SJ as double)") as "A0013R_SJ",
        expr("cast(A0013R_CS as int)") as "A0013R_CS")

    val df = data.groupBy("XLID").agg(
      min("A00110") as ("A00110"),
      sum("A00110_SJ") as ("A00110_SJ"),
      sum("A00110_CS") as ("A00110_CS"),
      sum(expr("case when A00110_CS is not null then 1 end")) as ("A00110_TS"),
      min("A00111") as ("A00111"),
      sum("A00111_SJ") as ("A00111_SJ"),
      sum("A00111_CS") as ("A00111_CS"),
      sum(expr("case when A00111_CS is not null then 1 end")) as ("A00111_TS"),
      min("A00115") as ("A00115"),
      sum("A00115_SJ") as ("A00115_SJ"),
      sum("A00115_CS") as ("A00115_CS"),
      sum(expr("case when A00115_CS is not null then 1 end")) as ("A00115_TS"),
      min("A00116") as ("A00116"),
      sum("A00116_SJ") as ("A00116_SJ"),
      sum("A00116_CS") as ("A00116_CS"),
      sum(expr("case when A00116_CS is not null then 1 end")) as ("A00116_TS"),
      min("A00118") as ("A00118"),
      sum("A00118_SJ") as ("A00118_SJ"),
      sum("A00118_CS") as ("A00118_CS"),
      sum(expr("case when A00118_CS is not null then 1 end")) as ("A00118_TS"),
      min("A00112") as ("A00112"),
      sum("A00112_SJ") as ("A00112_SJ"),
      sum("A00112_CS") as ("A00112_CS"),
      sum(expr("case when A00112_CS is not null then 1 end")) as ("A00112_TS"),
      min("A00130") as ("A00130"),
      sum("A00130_SJ") as ("A00130_SJ"),
      sum("A00130_CS") as ("A00130_CS"),
      sum(expr("case when A00130_CS is not null then 1 end")) as ("A00130_TS"),
      min("A00131") as ("A00131"),
      sum("A00131_SJ") as ("A00131_SJ"),
      sum("A00131_CS") as ("A00131_CS"),
      sum(expr("case when A00131_CS is not null then 1 end")) as ("A00131_TS"),
      min("A00132") as ("A00132"),
      sum("A00132_SJ") as ("A00132_SJ"),
      sum("A00132_CS") as ("A00132_CS"),
      sum(expr("case when A00132_CS is not null then 1 end")) as ("A00132_TS"),
      min("A00133") as ("A00133"),
      sum("A00133_SJ") as ("A00133_SJ"),
      sum("A00133_CS") as ("A00133_CS"),
      sum(expr("case when A00133_CS is not null then 1 end")) as ("A00133_TS"),
      min("A00134") as ("A00134"),
      sum("A00134_SJ") as ("A00134_SJ"),
      sum("A00134_CS") as ("A00134_CS"),
      sum(expr("case when A00134_CS is not null then 1 end")) as ("A00134_TS"),
      min("A00135") as ("A00135"),
      sum("A00135_SJ") as ("A00135_SJ"),
      sum("A00135_CS") as ("A00135_CS"),
      sum(expr("case when A00135_CS is not null then 1 end")) as ("A00135_TS"),
      min("A00136") as ("A00136"),
      sum("A00136_SJ") as ("A00136_SJ"),
      sum("A00136_CS") as ("A00136_CS"),
      sum(expr("case when A00136_CS is not null then 1 end")) as ("A00136_TS"),
      min("A00137") as ("A00137"),
      sum("A00137_SJ") as ("A00137_SJ"),
      sum("A00137_CS") as ("A00137_CS"),
      sum(expr("case when A00137_CS is not null then 1 end")) as ("A00137_TS"),
      min("A00138") as ("A00138"),
      sum("A00138_SJ") as ("A00138_SJ"),
      sum("A00138_CS") as ("A00138_CS"),
      sum(expr("case when A00138_CS is not null then 1 end")) as ("A00138_TS"),
      min("A00139") as ("A00139"),
      sum("A00139_SJ") as ("A00139_SJ"),
      sum("A00139_CS") as ("A00139_CS"),
      sum(expr("case when A00139_CS is not null then 1 end")) as ("A00139_TS"),
      min("A0013A") as ("A0013A"),
      sum("A0013A_SJ") as ("A0013A_SJ"),
      sum("A0013A_CS") as ("A0013A_CS"),
      sum(expr("case when A0013A_CS is not null then 1 end")) as ("A0013A_TS"),
      min("A0013R") as ("A0013R"),
      sum("A0013R_SJ") as ("A0013R_SJ"),
      sum("A0013R_CS") as ("A0013R_CS"),
      sum(expr("case when A0013R_CS is not null then 1 end")) as ("A0013R_TS"),
      min("A0013S") as ("A0013S"),
      sum("A0013S_SJ") as ("A0013S_SJ"),
      sum("A0013S_CS") as ("A0013S_CS"),
      sum(expr("case when A0013S_CS is not null then 1 end")) as ("A0013S_TS"),
      min("A0013T") as ("A0013T"),
      sum("A0013T_SJ") as ("A0013T_SJ"),
      sum("A0013T_CS") as ("A0013T_CS"),
      sum(expr("case when A0013T_CS is not null then 1 end")) as ("A0013T_TS"),

//      max(expr("null")) as ("A00112_XB"),
//      max(expr("null")) as ("A00112_ZDBPHD"),
//      max(expr("null")) as ("A00112_ZDBPHD_SJ"),
//      max(expr("null")) as ("A00118_XB"),
//      max(expr("null")) as ("A00118_ZDBPHD"),
//      max(expr("null")) as ("A00118_ZDBPHD_SJ"),

      max("WGQB") as ("WGQBC"),
      max("WGGB") as ("WGGBC"),
      sum("DYYCGDDS") as ("DYYCGDDS"),
      sum("DYYCGGDS") as ("DYYCGGDS"))

    val res = arch.join(df, Seq("XLID")).distinct()
    res
  }
}