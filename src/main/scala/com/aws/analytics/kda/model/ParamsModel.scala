package com.aws.analytics.kda.model

object ParamsModel {

  case class Params(awsRgeion: String, ak: String, sk: String, inputStreamName: String, streamInitPosition: String, streamInitialTimestamp: String, s3OutputPath: String)

  case class KafkaS3Params(brokerList:String,sourceTopic:String,consumerGroup:String,kafkaTableName:String,s3TableName:String ,s3TablePath:String)

  case class DataGenS3Params(dataGenTableName:String,s3TableName:String ,s3TablePath:String)

  case class KafkaHiveParams(brokerList:String,sourceTopic:String,consumerGroup:String,kafkaTableName:String,hiveMetastore:String,hiveDatabase:String,hiveTable:String,hiveTablePath:String)


}
