package com.bmsoft.impl

import scala.reflect.runtime.universe

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.countDistinct
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.functions.sum
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.hive.HiveContext

import com.bmsoft.util.DateFormatUtil
import com.bmsoft.util.JdbcConnUtil

/**
 * GD统计实现类
 * @author mathsyang
 */
class GDStatImpl(hc: HiveContext, val statDate: String, val statCycle: String) extends Serializable {
  def statGDDetail: DataFrame = {
    import hc.implicits._
    def statProv(org: DataFrame, gdDetail: DataFrame): DataFrame = {
      val dwbm = org.collect()(0).getAs[String]("DWBM")

      val res = gdDetail.groupBy("DQHJ", "GZYYFL", "JDFS", "SFWFD", "FDSC")
        .agg(countDistinct("OBJ_ID") as "GDSL",
          sum("YXGBSL") as "YXGBSL",
          sum("YXZBSL") as "YXZBSL",
          sum("YXYHS") as "YXYHSL",
          sum("QXSC") as "QXZSC_QXC",
          sum("QXSC") / countDistinct("OBJ_ID") as "QXPJSC_QXC",
          sum("QXSC_YHC") as "QXZSC_YHC",
          sum("QXSC_YHC") / countDistinct("OBJ_ID") as "QXPJSC_YHC").withColumn("DWBM", lit(dwbm)).join(org, Seq("DWBM"))

      res.select("DWBM", "DWMC", "DWJB", "SJDWBM", "SJDWMC", "DQHJ", "GZYYFL", "JDFS", "SFWFD", "FDSC",
        "GDSL", "YXGBSL", "YXZBSL", "YXYHSL", "QXZSC_QXC", "QXPJSC_QXC", "QXZSC_YHC", "QXPJSC_YHC")
    }

    def statCity(org: DataFrame, gdDetail: DataFrame): DataFrame = {
     val data = gdDetail.as("a").join(org.as("b"), $"a.SSGDDW" === $"b.DWBM")
        .withColumn("DWBM_TMP", expr("case when b.DWJB = '4' then b.DWBM when b.DWJB = '5' then b.SJDWBM end"))
        .drop("DWBM").drop("SSGDDW").withColumnRenamed("DWBM_TMP", "DWBM")

      val res = data.groupBy("DWBM", "DQHJ", "GZYYFL", "JDFS", "SFWFD", "FDSC")
        .agg(countDistinct("OBJ_ID") as "GDSL",
          sum("YXGBSL") as "YXGBSL",
          sum("YXZBSL") as "YXZBSL",
          sum("YXYHS") as "YXYHSL",
          sum("QXSC") as "QXZSC_QXC",
          sum("QXSC") / countDistinct("OBJ_ID") as "QXPJSC_QXC",
          sum("QXSC_YHC") as "QXZSC_YHC",
          sum("QXSC_YHC") / countDistinct("OBJ_ID") as "QXPJSC_YHC").join(org, Seq("DWBM"))
      res.select("DWBM", "DWMC", "DWJB", "SJDWBM", "SJDWMC", "DQHJ", "GZYYFL", "JDFS", "SFWFD", "FDSC",
        "GDSL", "YXGBSL", "YXZBSL", "YXYHSL", "QXZSC_QXC", "QXPJSC_QXC", "QXZSC_YHC", "QXPJSC_YHC")
    }

    def statTown(org: DataFrame, gdDetail: DataFrame): DataFrame = {
     val data = gdDetail.as("a").join(org.as("b"), $"a.SSGDDW" === $"b.DWBM").drop("SSGDDW")
      val res = data.groupBy("DWBM", "DWMC", "DWJB", "SJDWBM", "SJDWMC", "DQHJ", "GZYYFL", "JDFS", "SFWFD", "FDSC")
        .agg(countDistinct("OBJ_ID") as "GDSL",
          sum("YXGBSL") as "YXGBSL",
          sum("YXZBSL") as "YXZBSL",
          sum("YXYHS") as "YXYHSL",
          sum("QXSC") as "QXZSC_QXC",
          sum("QXSC") / countDistinct("OBJ_ID") as "QXPJSC_QXC",
          sum("QXSC_YHC") as "QXZSC_YHC",
          sum("QXSC_YHC") / countDistinct("OBJ_ID") as "QXPJSC_YHC")
      res.select("DWBM", "DWMC", "DWJB", "SJDWBM", "SJDWMC", "DQHJ", "GZYYFL", "JDFS", "SFWFD", "FDSC",
        "GDSL", "YXGBSL", "YXZBSL", "YXYHSL", "QXZSC_QXC", "QXPJSC_QXC", "QXZSC_YHC", "QXPJSC_YHC")
    }

    def statBZ(gdDetail: DataFrame): DataFrame = {
      val res = gdDetail.withColumnRenamed("BZSSGDDW", "DWBM")
        .withColumnRenamed("QXDW_MC", "DWMC")
        .groupBy("DWBM", "DWMC", "DQHJ", "GZYYFL", "JDFS", "SFWFD", "FDSC")
        .agg(countDistinct("OBJ_ID") as "GDSL",
          sum("YXGBSL") as "YXGBSL",
          sum("YXZBSL") as "YXZBSL",
          sum("YXYHS") as "YXYHSL",
          sum("QXSC") as "QXZSC_QXC",
          sum("QXSC") / countDistinct("OBJ_ID") as "QXPJSC_QXC",
          sum("QXSC_YHC") as "QXZSC_YHC",
          sum("QXSC_YHC") / countDistinct("OBJ_ID") as "QXPJSC_YHC")
        .withColumn("DWJB", lit(null))
        .withColumn("SJDWBM", lit(null))
        .withColumn("SJDWMC", lit(null))
      res.select("DWBM", "DWMC", "DWJB", "SJDWBM", "SJDWMC", "DQHJ", "GZYYFL", "JDFS", "SFWFD", "FDSC",
        "GDSL", "YXGBSL", "YXZBSL", "YXYHSL", "QXZSC_QXC", "QXPJSC_QXC", "QXZSC_YHC", "QXPJSC_YHC")
    }

    val dateFrom = DateFormatUtil.dateFrom(statDate, statCycle)
    val dateTo = DateFormatUtil.dateTo(dateFrom, statCycle)
    def tsToLong(ts: java.sql.Timestamp): java.lang.Long = if (ts == null) null else ts.getTime
    val tsToLongUDF = udf(tsToLong _)

    val sql = s"""select OBJ_ID,SSGDDW,BZSSGDDW,QXDW_MC,DQHJ,GZYYFL,JDFS,BXSJ,JLXFSJ,SFWFD,YXGBSL,YXZBSL,YXYHS,QXSC from PWYW_T_YJ_GZQX_QXD_DET
          where tjrq >= to_date(${dateFrom},'yyyymmdd') and tjrq < to_date(${dateTo},'yyyymmdd')"""

    val gdDetail = hc.read.jdbc(JdbcConnUtil.url, s"(${sql})", JdbcConnUtil.connProp)
      .withColumn("BXSJ_LONG", tsToLongUDF($"BXSJ")).withColumn("JLXFSJ_LONG", tsToLongUDF($"JLXFSJ"))
      .withColumn("QXSC_YHC", expr("(JLXFSJ_LONG - BXSJ_LONG)/1000/3600"))
      .withColumn("FDSC", expr("case when QXSC_YHC < 12 then '401' when QXSC_YHC < 24 then '402' else '403' end")).persist()

    val org = hc.sql("SELECT PMS_DWID DWBM,PMS_DWMC DWMC,PMS_DWCJ DWJB,SJDWID SJDWBM,SJDWMC FROM PWYW_ARCH.ST_PMS_YX_DW").persist()

    val prov = statProv(org.filter($"DWJB" === "3"), gdDetail).persist()
    val city = statCity(org.filter($"DWJB" isin ("4", "5")), gdDetail).persist()
    val town = statTown(org.filter($"DWJB" isin ("5", "8")), gdDetail).persist()
    val bz = statBZ(gdDetail.filter($"BZSSGDDW" isNotNull)).persist()

    val res = prov.unionAll(city).unionAll(town).unionAll(bz)

    res
  }
}