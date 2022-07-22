package com.aws.analytics.kda.model


object DataModel {

  //数据为JSON，两个字段,样例 {"deviceId":"a8xrtuyx","name":"aws-data"}
  case class Data(deviceId: String, name: String)

}
