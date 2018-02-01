package com.bmsoft.bean

case class DataBean(
    val ID: Option[String],
    val DATA_TIME: Option[String],
    val UA: Option[Double],
    val UB: Option[Double],
    val UC: Option[Double],
    val IA: Option[Double],
    val IB: Option[Double],
    val IC: Option[Double],
    val P: Option[Double],
    val Q: Option[Double],
    val F: Option[Double],
    val S: Option[Double],
    val T_FACTOR: Option[Double])