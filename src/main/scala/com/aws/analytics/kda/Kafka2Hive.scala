package com.aws.analytics.kda

import com.amazonaws.services.kinesisanalytics.runtime.KinesisAnalyticsRuntime
import com.aws.analytics.kda.util.ParameterToolUtils
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.runtime.state.StateBackend
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.table.api.{EnvironmentSettings, SqlDialect}
import org.apache.flink.table.catalog.hive.HiveCatalog
import org.apache.hadoop.hive.conf.HiveConf
import org.slf4j.{Logger, LoggerFactory}

object Kafka2Hive {

  val LOG: Logger = LoggerFactory.getLogger(Kafka2Hive.getClass)
  def main(args: Array[String]): Unit = {

    LOG.info("start run flink kafka2hive ....")
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
    val params = ParameterToolUtils.genKafka2HiveParams(parameter)

    // 创建 table env，流模式
    val settings = EnvironmentSettings.newInstance.useBlinkPlanner().inStreamingMode().build()
    val tEnv = StreamTableEnvironment.create(env, settings)

    // 设置Hive Catalog
    val name = "log-hive"
    val defaultDatabase = params.hiveDatabase
    val  hiveConf=new HiveConf()
    hiveConf.set("hive.metastore.uris",params.hiveMetastore)
    val hive = new HiveCatalog(name, defaultDatabase, hiveConf, "2.3.6")
    tEnv.registerCatalog(name, hive)
    tEnv.useCatalog(name)

    // 创建Flink Kafka 表SQL， 从timestamp生成event_time作为watermark
    val table = params.kafkaTableName
    val sourceTableSQL =
      s"""create table if not exists $table(
         |  uuid string,
         |  `date` string,
         |  ad_type int,
         |  ad_type_name string,
         |  `timestamp` bigint,
         |event_time AS TO_TIMESTAMP(FROM_UNIXTIME(`timestamp`/1000,'yyyy-MM-dd HH:mm:ss')),
         |WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
         |) with (
         |'connector' = 'kafka',
         |'topic' = '${params.sourceTopic}',
         |'properties.bootstrap.servers'='${params.brokerList}',
         |'properties.group.id' = '${params.consumerGroup}',
         |'format' = 'json',
         |'json.fail-on-missing-field' = 'false',
         |'json.ignore-parse-errors' = 'true'
         |)
      """.stripMargin
    // 创建表，会在hive metastore中存储
    tEnv.executeSql(sourceTableSQL)


    // 切换为hive dialect, 创建hive表，设置为三级分区，day,hour,min
    tEnv.getConfig.setSqlDialect(SqlDialect.HIVE)
    val hiveTable = params.hiveTable
    // 其中hiveTablePath可以是S3路径，也可以是HDFS路径，如果为HDFS路径，必须在项目的resource目录中添加core-site.xml和hdfs-site.xml
    // 两个xml需要的hdfs信息，参看本项目的resource目录中的配置信息即可
    val createHiveTableSQL =
    s"""
       |  CREATE  EXTERNAL TABLE  if not exists $hiveTable(
       |  uuid string,
       | `date` string,
       |  ad_type int,
       |  ad_type_name string,
       | `timestamp` bigint
       |    ) PARTITIONED BY (logday STRING,h STRING,m STRING)
       |    STORED AS parquet
       |    LOCATION '${params.hiveTablePath}'
       |    TBLPROPERTIES (
       |    'partition.time-extractor.timestamp-pattern' = '$$logday $$h:$$m:00',
       |    'sink.partition-commit.trigger'='partition-time',
       |    'sink.partition-commit.delay'='1 min',
       |    'sink.partition-commit.policy.kind'='metastore',
       |    'sink.rolling-policy.rollover-interval'='1 min',
       |    'sink.rolling-policy.file-size'='128MB',
       |    'auto-compaction'='true'
       |    )
       |
      """.stripMargin
    tEnv.executeSql(createHiveTableSQL)

    //insert data to hive
    tEnv.getConfig.setSqlDialect(SqlDialect.DEFAULT)
    val insertDataSQL =
      s"""
         |INSERT INTO  $hiveTable
         |SELECT
         |  uuid ,
         | `date` ,
         |  ad_type ,
         |  ad_type_name ,
         | `timestamp` ,
         |DATE_FORMAT(event_time, 'yyyy-MM-dd') as logday,
         |DATE_FORMAT(event_time, 'HH') as h,
         |DATE_FORMAT(event_time,'mm') as m
         |FROM $table
      """.stripMargin
    tEnv.executeSql(insertDataSQL)
  }
}
