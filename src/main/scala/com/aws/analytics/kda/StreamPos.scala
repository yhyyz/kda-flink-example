package com.aws.analytics.kda

object StreamPos extends Enumeration {

  type StreamPos = Value
  //枚举的定义
  val LATEST, TRIM_HORIZON, AT_TIMESTAMP = Value
}
