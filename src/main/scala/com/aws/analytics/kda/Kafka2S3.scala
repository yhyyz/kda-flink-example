package com.aws.analytics.kda

import com.amazonaws.services.kinesisanalytics.runtime.KinesisAnalyticsRuntime
import com.aws.analytics.kda.util.ParameterToolUtils
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.table.catalog.GenericInMemoryCatalog
import org.slf4j.{Logger, LoggerFactory}


object Kafka2S3 {

  val LOG: Logger = LoggerFactory.getLogger(Kafka2S3.getClass)
  def main(args: Array[String]): Unit = {

//    LOG.info(args.mkString(" ,"))
//    // 解析命令行参数
    LOG.info("Kafka2S3: start run kda flink table api  kafka data to s3 ....")
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // 注意在Kinesis Analysis 运行时中该参数不生效，需要在CLI中设置相关参数，同时KDA 默认会使用RocksDB存储状态，不用设置
    env.enableCheckpointing(10000)
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)

    var parameter: ParameterTool = null
    if (env.getJavaEnv.getClass == classOf[LocalStreamEnvironment]) {
      parameter = ParameterTool.fromArgs(args)
    } else {
      // 使用KDA Runtime获取参数
      val applicationProperties = KinesisAnalyticsRuntime.getApplicationProperties.get("FlinkTableAppProperties")
      if (applicationProperties == null) {
        throw new RuntimeException("Unable to load properties from Group ID FlinkTableAppProperties.")
      }
      parameter = ParameterToolUtils.fromApplicationProperties(applicationProperties)
    }
    val params = ParameterToolUtils.genKafka2S3Params(parameter)

    // 创建 table env，流模式
    val settings = EnvironmentSettings.newInstance.useBlinkPlanner().inStreamingMode().build()
    val tEnv = StreamTableEnvironment.create(env,settings)
    // 设置Hive Catalog
    val catalogName = "kafka-memory"
    val database = "default"
    val memoryCatalog = new GenericInMemoryCatalog(catalogName,database)
    tEnv.registerCatalog(catalogName, memoryCatalog)
    tEnv.useCatalog(catalogName)

    // 创建Flink Kafka 表SQL， 从ctime生成event_time作为watermark
    val kafkaTable = params.kafkaTableName
    val sourceKafkaTableSQL =
      s"""
         |create table  $kafkaTable(
         |  deviceId string,
         |  name string,
         |  ctime bigint,
         |event_time AS TO_TIMESTAMP(FROM_UNIXTIME(ctime/1000,'yyyy-MM-dd HH:mm:ss')),
         |WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
         |)with(
         |'connector' = 'kafka',
         |'topic' = '${params.sourceTopic}',
         |'properties.bootstrap.servers'='${params.brokerList}',
         |'properties.group.id' = '${params.consumerGroup}',
         |'format' = 'json'
         |)
      """.stripMargin

    // 创建表，会在hive metastore中存储
    tEnv.executeSql(sourceKafkaTableSQL)
//    tEnv.executeSql("select * from sources_k").print()
    val s3Table=params.s3TableName
    val createS3TableSQL=
      s"""CREATE TABLE  $s3Table"""+"""(
      |       deviceId string,
      |       name string,
      |       ctime bigint,
      |       logday string,
      |       h string
      |    ) PARTITIONED BY (logday ,h ) with (
      |    'connector' = 'filesystem',
      |    'format' = 'parquet',
      |    'partition.time-extractor.timestamp-pattern' = '$logday $h:00:00',
      |    'sink.partition-commit.trigger'='partition-time',
      |    'sink.partition-commit.delay'='1 h',
      |    'sink.partition-commit.policy.kind'='success-file',
      |    'sink.rolling-policy.rollover-interval'='10 min',
      |    'sink.rolling-policy.file-size'='128MB',
      |    'auto-compaction'='true',
      |    'path' =""".stripMargin+s""" '${params.s3TablePath}'
      |    )
      """.stripMargin

    tEnv.executeSql(createS3TableSQL)

    // 流式插入数据
    val insertDataSQL=
      s"""
         |INSERT INTO  $s3Table SELECT
         |  deviceId,
         |  name,
         |  ctime,
         |DATE_FORMAT(event_time, 'yyyy-MM-dd') as logday,
         |DATE_FORMAT(event_time, 'HH') as h
         |FROM $kafkaTable
      """.stripMargin

    tEnv.executeSql(insertDataSQL)

  }

}
