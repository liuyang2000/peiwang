package com.bmsoft.bean

class OracleRunStatusBean(val ID: Option[String],
    val ZDYGGL: Option[Double],
    val ZDYGGL_SJ: Option[String],
    val ZXYGGL: Option[Double],
    val ZXYGGL_SJ: Option[String],
    val ZDSZGL: Option[Double],
    val ZDSZGL_SJ: Option[String],
    val ZXGLYS: Option[Double],
    val ZXGLYS_SJ: Option[String],
    val ZDDL_A: Option[Double],
    val ZDDL_A_SJ: Option[String],
    val ZXDL_A: Option[Double],
    val ZXDL_A_SJ: Option[String],
    val ZDDL_B: Option[Double],
    val ZDDL_B_SJ: Option[String],
    val ZXDL_B: Option[Double],
    val ZXDL_B_SJ: Option[String],
    val ZDDL_C: Option[Double],
    val ZDDL_C_SJ: Option[String],
    val ZXDL_C: Option[Double],
    val ZXDL_C_SJ: Option[String],
    val ZXDY_A: Option[Double],
    val ZXDY_A_SJ: Option[String],
    val ZXDY_B: Option[Double],
    val ZXDY_B_SJ: Option[String],
    val ZXDY_C: Option[Double],
    val ZXDY_C_SJ: Option[String],
    val ZDDY_A: Option[Double],
    val ZDDY_A_SJ: Option[String],
    val ZDDY_B: Option[Double],
    val ZDDY_B_SJ: Option[String],
    val ZDDY_C: Option[Double],
    val ZDDY_C_SJ: Option[String],
    val PJFH: Option[Double],
    val FHL: Option[Double],
    val AXDYQXYCDS: Option[Int],
    val AXDYQXSCDS: Option[Int],
    val BXDYQXYCDS: Option[Int],
    val BXDYQXSCDS: Option[Int],
    val CXDYQXYCDS: Option[Int],
    val CXDYQXSCDS: Option[Int],
    val AXDLQXYCDS: Option[Int],
    val AXDLQXSCDS: Option[Int],
    val BXDLQXYCDS: Option[Int],
    val BXDLQXSCDS: Option[Int],
    val CXDLQXYCDS: Option[Int],
    val CXDLQXSCDS: Option[Int],
    val GLYSQXYCDS: Option[Int],
    val GLYSQXSCDS: Option[Int],
    val YGZGLQXYCDS: Option[Int],
    val YGZGLQXSCDS: Option[Int],
    val WGZGLQXYCDS: Option[Int],
    val WGZGLQXSCDS: Option[Int],
    val FZL :Option[Double]) extends Product with Serializable {
  override def productElement(n: Int) = n match {
    case 0 => ID
    case 1 => ZDYGGL
    case 2 => ZDYGGL_SJ
    case 3 => ZXYGGL
    case 4 => ZXYGGL_SJ
    case 5 => ZDSZGL
    case 6 => ZDSZGL_SJ
    case 7 => ZXGLYS
    case 8 => ZXGLYS_SJ
    case 9 => ZDDL_A
    case 10 => ZDDL_A_SJ
    case 11 => ZXDL_A
    case 12 => ZXDL_A_SJ
    case 13 => ZDDL_B
    case 14 => ZDDL_B_SJ
    case 15 => ZXDL_B
    case 16 => ZXDL_B_SJ
    case 17 => ZDDL_C
    case 18 => ZDDL_C_SJ
    case 19 => ZXDL_C
    case 20 => ZXDL_C_SJ
    case 21 => ZDDY_A
    case 22 => ZDDY_A_SJ
    case 23 => ZDDY_B
    case 24 => ZDDY_B_SJ
    case 25 => ZDDY_C
    case 26 => ZDDY_C_SJ
    case 27 => ZDDY_A
    case 28 => ZDDY_A_SJ
    case 29 => ZDDY_B
    case 30 => ZDDY_B_SJ
    case 31 => ZDDY_C
    case 32 => ZDDY_C_SJ
    case 33 => PJFH
    case 34 => FHL
    case 35 => AXDYQXYCDS
    case 36 => AXDYQXSCDS
    case 37 => BXDYQXYCDS
    case 38 => BXDYQXSCDS
    case 39 => CXDYQXYCDS
    case 40 => CXDYQXSCDS
    case 41 => AXDLQXYCDS
    case 42 => AXDLQXSCDS
    case 43 => BXDLQXYCDS
    case 44 => BXDLQXSCDS
    case 45 => CXDLQXYCDS
    case 46 => CXDLQXSCDS
    case 47 => GLYSQXYCDS
    case 48 => GLYSQXSCDS
    case 49 => YGZGLQXYCDS
    case 50 => YGZGLQXSCDS
    case 51 => WGZGLQXYCDS
    case 52 => WGZGLQXSCDS
    case 53 => FZL
  }

  override def productArity: Int = 54

  override def canEqual(that: Any): Boolean = that.isInstanceOf[OracleRunStatusBean]
}
