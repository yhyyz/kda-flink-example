package com.aws.analytics.kda.util

import com.aws.analytics.kda.model.ParamsModel
import org.apache.flink.api.java.utils.ParameterTool

import java.util
import java.util.Properties

object ParameterToolUtils {


  def fromApplicationProperties(properties: Properties): ParameterTool = {
    val map = new util.HashMap[String, String](properties.size())
    properties.forEach((k, v) => map.put(String.valueOf(k), String.valueOf(v)))
    ParameterTool.fromMap(map)
  }

  def genParams(parameter: ParameterTool): ParamsModel.Params = {
    val aws_region = parameter.get("aws_region")
    val ak = parameter.get("ak")
    val sk = parameter.get("sk")
    val inputStreamName = parameter.get("input_stream_name")
    val stream_init_position = parameter.get("stream_init_position")
    val streamInitialTimestamp = parameter.get("stream_initial_timestamp")
    val s3_output_path = parameter.get("s3_output_path")
    val params = ParamsModel.Params.apply(aws_region, ak, sk, inputStreamName, stream_init_position, streamInitialTimestamp, s3_output_path)
    params
  }
  def genKafka2S3Params(parameter: ParameterTool): ParamsModel.KafkaS3Params = {
    val brokerList = parameter.get("broker_list")
    val sourceTopic = parameter.get("source_topic")
    val consumerGroup = parameter.get("consumer_group")
    val kafkaTableName  = parameter.get("kafka_tablename")
    val s3TableName  = parameter.get("s3_table_name")
    val s3TablePath  = parameter.get("s3_table_path")
    val params = ParamsModel.KafkaS3Params.apply(brokerList,sourceTopic,consumerGroup,kafkaTableName,s3TableName,s3TablePath)
    params
  }

  def genKafka2HiveParams(parameter: ParameterTool): ParamsModel.KafkaHiveParams = {
    val brokerList = parameter.get("broker_list")
    val sourceTopic = parameter.get("source_topic")
    val consumerGroup = parameter.get("consumer_group")
    val kafkaTableName  = parameter.get("kafka_tablename")
    val hiveMetastore  = parameter.get("hive_metastore")
    val hiveDatabase = parameter.get("hive_database")
    val hiveTable = parameter.get("hive_table")
    val hiveTablePath  = parameter.get("hive_table_path")
    val params = ParamsModel.KafkaHiveParams.apply(brokerList,sourceTopic,consumerGroup,kafkaTableName,hiveMetastore,hiveDatabase,hiveTable,hiveTablePath)
    params
  }

  def genDataGen2S3Params(parameter: ParameterTool): ParamsModel.DataGenS3Params = {

    val dataGenName  = parameter.get("datagen_table_name")
    val s3TableName  = parameter.get("s3_table_name")
    val s3TablePath  = parameter.get("s3_table_path")
    val params = ParamsModel.DataGenS3Params.apply(dataGenName,s3TableName,s3TablePath)
    params
  }

  def genDataGen2HudiParams(parameter: ParameterTool): ParamsModel.DataGenS3Params = {

    val dataGenName  = parameter.get("datagen_table_name")
    val hudiTableName  = parameter.get("hudi_table_name")
    val hudiTablePath  = parameter.get("hudi_table_path")
    val params = ParamsModel.DataGenS3Params.apply(dataGenName,hudiTableName,hudiTablePath)
    params
  }
}
