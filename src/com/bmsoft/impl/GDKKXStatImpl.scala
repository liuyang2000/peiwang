package com.bmsoft.impl

import org.apache.spark.SparkConf
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.SparkContext
import java.text.SimpleDateFormat
import java.util.Date
import com.bmsoft.util.JdbcConnUtil
import org.apache.spark.sql.DataFrame
import java.sql.DriverManager
import java.sql.PreparedStatement
import java.sql.Connection
import org.apache.spark.sql.DataFrame
import com.bmsoft.util.JdbcConnUtil
import org.apache.spark.sql.functions._
import java.math.BigDecimal
import com.bmsoft.bean.RunStatusBean
import com.bmsoft.bean.DataBean
import com.bmsoft.util.DateFormatUtil
import com.bmsoft.util.CommProperties

/**
 * GDKKX统计实现类
 * @author mathsyang
 */
class GDKKXStatImpl(hc: HiveContext, val statDate: String, val statCycle: String) extends Serializable {
  /**
   * gdkkx stat implements
   * @param org
   * @param kgDetail
   * @return
   */
  def statGDKKX: DataFrame = {
    import hc.implicits._
    def statProv(org: DataFrame, df: DataFrame): DataFrame = {
      val dwbm = org.collect()(0).getAs[String]("DWBM")

      val res = df.groupBy("CNW")
        .agg(countDistinct("OBJ_ID") as "DEV_CNT")
        .withColumn("DWBM", lit(dwbm))
      selectResult(res)
    }

    def statCity(org: DataFrame, df: DataFrame): DataFrame = {
      val data = df.as("a").join(org.as("b"), $"a.SSDS" === $"b.DWBM")
        .withColumn("DWBM", expr("case when b.DWJB = '4' then b.DWBM when b.DWJB = '5' then b.SJDWBM end"))
        .drop("SSDS")

      val res = data.groupBy("DWBM", "CNW")
        .agg(countDistinct("OBJ_ID") as "DEV_CNT")
      selectResult(res)
    }

    def statTown(org: DataFrame, df: DataFrame): DataFrame = {
      val data = df.as("a").join(org.as("b"), $"a.WHBZ" === $"b.DWBM")
        .withColumn("DWBM", expr("case when b.DWJB = '5' then b.DWBM else b.SJDWBM end"))
        .drop("WHBZ")

      val res = data.groupBy("DWBM", "CNW")
        .agg(countDistinct("OBJ_ID") as "DEV_CNT")
      selectResult(res)
    }

    def statBZ(org: DataFrame, df: DataFrame): DataFrame = {
      val res = df.withColumnRenamed("WHBZ", "DWBM").join(org, Seq("DWBM"))
        .groupBy("DWBM", "CNW")
        .agg(countDistinct("OBJ_ID") as "DEV_CNT")
      selectResult(res)
    }

    def selectResult(df: DataFrame): DataFrame = {
      df.select("DWBM", "CNW", "DEV_CNT")
    }

    val dateFrom = DateFormatUtil.dateFrom(statDate, statCycle)
    val dateTo = DateFormatUtil.dateTo(dateFrom, statCycle)

    val tableSS = DateFormatUtil.tableName(s"PWYW_DWGDKKXTJ_SS", statCycle)
    val tableBZ = DateFormatUtil.tableName(s"PWYW_DWGDKKXTJ_BZ", statCycle)
    val dataCondition = conditionStr(statDate)
    val sqlSS = s"""SELECT DWBM,CNW,ZYJHTDSC + ZYLSJXTDSC + ZYGZTDSC TDZSC,DQGDKKX DQGDKKX_PRE, 'SS' SSBZ FROM ${tableSS} where ${dataCondition}"""
    val sqlBZ = s"""SELECT DWBM,CNW,ZYJHTDSC + ZYLSJXTDSC + ZYGZTDSC TDZSC,DQGDKKX DQGDKKX_PRE, 'BZ' SSBZ FROM ${tableBZ} where ${dataCondition}"""
    val gdkkx = hc.read.jdbc(JdbcConnUtil.url, s"(${sqlSS})", JdbcConnUtil.connProp)
      .unionAll(hc.read.jdbc(JdbcConnUtil.url, s"(${sqlBZ})", JdbcConnUtil.connProp)).persist()

    val dev_det = hc.sql("SELECT SSDS,WHBZ,CASE WHEN SFNW = '0' THEN '2' ELSE '3' END CNW, OBJ_ID FROM pwyw_arch.t_sb_zwyc_zsbyq")
      .unionAll(hc.sql("SELECT SSDS,WHBZ,CASE WHEN SFNW = '0' THEN '2' ELSE '3' END CNW, OBJ_ID FROM pwyw_arch.t_sb_znyc_pdbyq")).distinct().persist()
    val dev_cnw_all = dev_det.withColumn("CNW", lit("1"))
    val dev = dev_det.unionAll(dev_cnw_all)

    val org = hc.sql("SELECT PMS_DWID DWBM,PMS_DWCJ DWJB,SJDWID SJDWBM FROM PWYW_ARCH.ST_PMS_YX_DW").persist()

    val prov = statProv(org.filter($"DWJB" === "3"), dev).persist()
    val city = statCity(org.filter($"DWJB" isin ("4", "5")), dev).persist()
    val town = statTown(org.filter($"DWJB" isin ("5", "8")), dev).persist()
    val bz = statBZ(org.filter($"DWJB" === "8"), dev).persist()

    val devGroup = prov.unionAll(city).unionAll(town).unionAll(bz)

    val statHours = DateFormatUtil.statHours(dateFrom, statCycle)
    val yearToNowHours = DateFormatUtil.yearToNowHours(dateFrom)
    val res = gdkkx.join(devGroup, Seq("DWBM", "CNW"))
      .withColumn("GDKKX", expr(s"round(100 * (1 - TDZSC / DEV_CNT / ${statHours}),5)"))
      .withColumn("DQGDKKX",expr(s"case when DQGDKKX_PRE is null then GDKKX else (DQGDKKX_PRE * ${yearToNowHours} + GDKKX * ${statHours})/ (${yearToNowHours} + ${statHours}) end"))
      .drop("DEV_CNT").drop("TDZSC")

    res
  }

  def conditionStr(dateStr: String): String = {
    if (DateFormatUtil.STAT_CYCLE_WEEK.equals(statCycle)) {
      s""" tjksrq = to_date('${DateFormatUtil.dateFrom(dateStr, statCycle)}','${CommProperties.FORMAT_DATE_DAY}') """
    } else if (DateFormatUtil.STAT_CYCLE_DAY.equals(statCycle)) {
      s""" tjrq = to_date('${dateStr}','${CommProperties.FORMAT_DATE_DAY}') """
    } else {
      s""" tjrq = '${dateStr.substring(0, 6)}' """
    }
  }

  /**
   * tz stat implements
   */
  def statTZ: DataFrame = {
    import hc.implicits._

    def aggTZStat(data: DataFrame): DataFrame = {
      val res = data.groupBy("CNW", "DWBM").agg(countDistinct("SBID") as "TZKGSL",
        count("SBID") as "TZCS",
        count("SBID") / sum("XLZCD") as "BGL_TZCS",
        sum(expr("case when XLXZ = '1' then 1 else 0 end")) as "ZX_TZCS",
        sum(expr("case when XLXZ = '2' then 1 else 0 end")) as "ZHIX_TZCS",
        sum(expr("case when XLXZ = '3' then 1 else 0 end")) as "FZX_TZCS",
        sum(expr("case when XLXZ = '4' then 1 else 0 end")) as "FDX_TZCS",
        sum(expr("case when XLXZ = '5' then 1 else 0 end")) as "KX_TZCS")
      res
    }

    def selectFrame(data: DataFrame, ssbz: String) = {
      data.select("DWBM", "DWMC", "DWJB", "SJDWBM", "SJDWMC", "CNW", "TZKGSL", "TZCS", "BGL_TZCS", "ZX_TZCS", "ZHIX_TZCS", "FZX_TZCS", "FDX_TZCS", "KX_TZCS").withColumn("SSBZ", lit(ssbz))
    }

    def statProv(org: DataFrame, kgDetail: DataFrame): DataFrame = {
      val dwbm = org.collect()(0).getAs[String]("DWBM")

      val res = aggTZStat(kgDetail.withColumn("DWBM", lit(dwbm))).join(org, Seq("DWBM"))

      selectFrame(res, "SS")
    }

    def statCity(org: DataFrame, kgDetail: DataFrame): DataFrame = {
      import hc.implicits._
      val data = kgDetail.as("a").join(org.as("b"), $"a.DWID" === $"b.DWBM")
        .withColumn("DWBM", expr("case when b.DWJB = '4' then b.DWBM when b.DWJB = '5' then b.SJDWBM end"))
        .drop("DWID")

      val res = aggTZStat(data).join(org, Seq("DWBM"))

      selectFrame(res, "SS")
    }

    def statTown(org: DataFrame, kgDetail: DataFrame): DataFrame = {
      import hc.implicits._
      val data = kgDetail.as("a").join(org.as("b"), $"a.DWID" === $"b.DWBM").drop("DWID")
      val res = aggTZStat(data).join(org, Seq("DWBM"))

      selectFrame(res, "BZ")
    }

    //    def statBZ(kgDetail: DataFrame): DataFrame = {
    //      val res = kgDetail.withColumnRenamed("BZSSGDDW", "DWBM")
    //        .withColumnRenamed("QXDW_MC", "DWMC")
    //        .groupBy("")
    //        .agg(countDistinct("") as "")
    //      selectFrame(res)
    //    }

    val dateFrom = DateFormatUtil.dateFrom(statDate, statCycle)
    val dateTo = DateFormatUtil.dateTo(dateFrom, statCycle)
    //    def tsToLong(ts: java.sql.Timestamp): java.lang.Long = if (ts == null) null else ts.getTime
    //    val tsToLongUDF = udf(tsToLong _)

    val sql_kgzt = s"""select * from pwyw_t_dms_switch_state where kgzt = '0' and kgdzsj >= to_date(${dateFrom},'yyyymmdd') and kgdzsj < to_date(${dateTo},'yyyymmdd')"""
    val kgzt = hc.read.jdbc(JdbcConnUtil.url, s"(${sql_kgzt})", JdbcConnUtil.connProp).persist()
    val kgxx = hc.read.jdbc(JdbcConnUtil.url, """(select OBJ_ID SBID,DWID,SFNW,SSXL from st_pms_kg_dev)""", JdbcConnUtil.connProp).persist()
    val kgzp = hc.read.jdbc(JdbcConnUtil.url,
      s"""(select SBID FSZP from pwyw_t_dms_plate_state where czsj >= to_date(${dateFrom},'yyyymmdd') and czsj < to_date(${dateTo},'yyyymmdd'))""", JdbcConnUtil.connProp).persist()
    val kgxl = hc.sql("select OBJ_ID SSXL, XLXZ, XLZCD from pwyw_arch.t_sb_zwyc_xl")

    val kgDetail_det = kgzt.join(kgxx, Seq("SBID")).as("a")
      .join(kgzp.as("b"), $"a.SBID" === $"b.FSZP", "left_outer")
      .join(kgxl.as("c"), Seq("SSXL"), "left_outer").filter($"FSZP" isNull)
      .withColumn("CNW", expr("case when SFNW = '0' then '2' else '3' end")).persist()
    val kgDetail_all = kgDetail_det.withColumn("CNW", lit("1"))
    val kgDetail = kgDetail_det.unionAll(kgDetail_all)

    val org = hc.sql("SELECT PMS_DWID DWBM,PMS_DWMC DWMC,PMS_DWCJ DWJB,SJDWID SJDWBM,SJDWMC FROM PWYW_ARCH.ST_PMS_YX_DW").persist()

    val prov = statProv(org.filter($"DWJB" === "3"), kgDetail).persist()
    val city = statCity(org.filter($"DWJB" isin ("4", "5")), kgDetail).persist()
    val town = statTown(org.filter($"DWJB" isin ("5", "8")), kgDetail).persist()
    //    val bz = statBZ(kgDetail.filter($"BZSSGDDW" isNotNull)).persist()

    val res_0 = prov.unionAll(city).unionAll(town).persist() //.unionAll(bz)

    val preStatDate = DateFormatUtil.datePre(dateFrom, statCycle)
    val preDataCondition = conditionStr(preStatDate)
    val tableName = DateFormatUtil.tableName(s"PWYW_DWTZTJ", statCycle)
    //    val tabBZ = DateFormatUtil.tableName(s"PWYW_DWGDKKXTJ_BZ", statCycle)
    val preTZStatData = hc.read.jdbc(JdbcConnUtil.url, s"(SELECT DWBM, CNW, TZKGSL, TZCS, BGL_TZCS, LJ_TZCS FROM ${tableName} where ${preDataCondition})", JdbcConnUtil.connProp).persist()
    val curYear = DateFormatUtil.yearOfDate(statDate)
    val preYear = DateFormatUtil.yearOfDate(preStatDate)
    val res_1 =
      if (curYear.equals(preYear)) {
        res_0.as("a").join(preTZStatData.select("DWBM", "CNW", "LJ_TZCS")
          .withColumnRenamed("DWBM", "DWBM_B").withColumnRenamed("CNW", "CNW_B").as("b"),
          expr("a.DWBM = DWBM_B and a.CNW = b.CNW_B"), "left_outer")
          .withColumn("LJ_TZCS", expr("a.TZCS + (case when b.LJ_TZCS is null then 0 else b.LJ_TZCS end)"))
          .drop("DWBM_B").drop("CNW_B")
      } else {
        res_0.withColumn("LJ_TZCS", $"TZCS")
      }

    val res = if (DateFormatUtil.STAT_CYCLE_MON.equals(statCycle) || DateFormatUtil.STAT_CYCLE_SEASON.equals(statCycle)) {
      val preYearStatDate = DateFormatUtil.preYear(dateFrom)
      val preYearDataCondition = conditionStr(preYearStatDate)
      val preYearTZStatData = hc.read.jdbc(JdbcConnUtil.url, s"(SELECT DWBM, CNW, TZKGSL, TZCS, BGL_TZCS FROM ${tableName} where ${preYearDataCondition})", JdbcConnUtil.connProp).persist()

      res_1.as("a").join(preTZStatData.drop("LJ_TZCS")
        .withColumnRenamed("DWBM", "DWBM_B")
        .withColumnRenamed("CNW", "CNW_B")
        .withColumnRenamed("TZKGSL", "TZKGSL_B")
        .withColumnRenamed("TZCS", "TZCS_B")
        .withColumnRenamed("BGL_TZCS", "BGL_TZCS_B")
        .as("b"), expr("a.DWBM = b.DWBM_B and a.CNW = b.CNW_B"), "left_outer")
        .join(preYearTZStatData
          .withColumnRenamed("DWBM", "DWBM_C")
          .withColumnRenamed("CNW", "CNW_C")
          .withColumnRenamed("TZKGSL", "TZKGSL_C")
          .withColumnRenamed("TZCS", "TZCS_C")
          .withColumnRenamed("BGL_TZCS", "BGL_TZCS_C")
          .as("c"), expr("a.DWBM = c.DWBM_C and a.CNW = c.CNW_C"), "left_outer")
        .withColumn("TZKGSL_TB", expr("case when c.TZKGSL_C is null or c.TZKGSL_C = 0 then null else a.TZKGSL / c.TZKGSL_C end "))
        .withColumn("TZKGSL_HB", expr("case when b.TZKGSL_B is null or b.TZKGSL_B = 0 then null else a.TZKGSL / b.TZKGSL_B end "))
        .withColumn("TZCS_TB", expr("case when c.TZCS_C is null or c.TZCS_C = 0 then null else a.TZCS / c.TZCS_C end "))
        .withColumn("TZCS_HB", expr("case when b.TZCS_B is null or b.TZCS_B = 0 then null else a.TZCS / b.TZCS_B end "))
        .withColumn("BGL_TZCS_TB", expr("case when c.BGL_TZCS_C is null or c.BGL_TZCS_C = 0 then null else a.BGL_TZCS / c.BGL_TZCS_C end "))
        .withColumn("BGL_TZCS_HB", expr("case when b.BGL_TZCS_B is null or b.BGL_TZCS_B = 0 then null else a.BGL_TZCS / b.BGL_TZCS_B end "))
        .drop("b.DWBM_B").drop("b.CNW_B").drop("b.TZKGSL_B").drop("b.TZCS_B").drop("b.BGL_TZCS_B")
        .drop("c.DWBM_C").drop("c.CNW_C").drop("c.TZKGSL_C").drop("c.TZCS_C").drop("c.BGL_TZCS_C").persist()
    } else {
      res_1.withColumn("TZKGSL_TB", lit(null))
        .withColumn("TZKGSL_HB", lit(null))
        .withColumn("TZCS_TB", lit(null))
        .withColumn("TZCS_HB", lit(null))
        .withColumn("BGL_TZCS_TB", lit(null))
        .withColumn("BGL_TZCS_HB", lit(null)).persist()
    }

    //    val dataCondition = conditionStr(statDate)
    //    val gdkkxData = hc.read.jdbc(JdbcConnUtil.url, s"(SELECT DWBM, CNW FROM ${tabSS} where ${dataCondition})", JdbcConnUtil.connProp)
    //      .unionAll(hc.read.jdbc(JdbcConnUtil.url, s"(SELECT DWBM, CNW FROM ${tabBZ} where ${dataCondition})", JdbcConnUtil.connProp)).persist()
    //
    //    val res = res_2.as("a").join(gdkkxData.as("b"), expr("a.DWBM = b.DWBM and a.CNW = b.CNW"), "left_outer")
    //      .withColumn("SAVE_UPDATE", expr("case when b.DWBM is null and b.CNW is null then 'SAVE' else 'UPDATE' end"))
    //      .drop("b.DWBM").drop("b.CNW")

    res
  }
}