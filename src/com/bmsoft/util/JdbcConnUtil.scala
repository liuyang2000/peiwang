package com.bmsoft.util

import java.util.Date
import java.util.Calendar
import java.text.SimpleDateFormat
import java.util.Properties

object JdbcConnUtil {
  val BATCH_COMMIT_CNT = 1000
  val driver = "oracle.jdbc.driver.OracleDriver"
  val url = "jdbc:oracle:thin:@//10.217.14.203:11521/sgpms"
  val userName = "pwyw"
  val passwd = "nxpms_pwyw!"
  val connProp = new Properties()
  connProp.setProperty("driver", driver)
  connProp.setProperty("user", userName)
  connProp.setProperty("password", passwd)

  val sdfDay = new SimpleDateFormat("yyyyMMdd")
}