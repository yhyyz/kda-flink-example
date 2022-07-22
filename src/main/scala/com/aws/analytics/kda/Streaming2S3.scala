/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.aws.analytics.kda

import com.amazonaws.services.kinesisanalytics.runtime.KinesisAnalyticsRuntime
import com.aws.analytics.kda.model.{DataModel, ParamsModel}
import com.aws.analytics.kda.util.ParameterToolUtils
import com.google.gson.GsonBuilder
import org.apache.flink.api.common.serialization.{SimpleStringEncoder, SimpleStringSchema}
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.core.fs.Path
import org.apache.flink.formats.parquet.avro.ParquetAvroWriters
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kinesis.FlinkKinesisConsumer
import org.apache.flink.streaming.connectors.kinesis.config.{AWSConfigConstants, ConsumerConfigConstants}
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.logging.log4j.LogManager

import java.util.Properties
import java.util.concurrent.TimeUnit

/**
 * Steaming to S3
 */
object Streaming2S3 {

  private val log = LogManager.getLogger(Streaming2S3.getClass)
  private val gson = new GsonBuilder().create

  def main(args: Array[String]) {
    // set up the streaming execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // 注意在Kinesis Analysis 运行时中该参数不生效，需要在CLI中设置相关参数，同时KDA 默认会使用RocksDB存储状态，不用设置
    env.enableCheckpointing(5000)
    var parameter: ParameterTool = null
    if (env.getJavaEnv.getClass == classOf[LocalStreamEnvironment]) {
      parameter = ParameterTool.fromArgs(args)
    } else {
      // 使用KDA Runtime获取ch数
      val applicationProperties = KinesisAnalyticsRuntime.getApplicationProperties.get("FlinkAppProperties")
      if (applicationProperties == null) {
        throw new RuntimeException("Unable to load properties from Group ID FlinkAppProperties.")
      }
      parameter = ParameterToolUtils.fromApplicationProperties(applicationProperties)
    }
    val params = ParameterToolUtils.genParams(parameter)

    val source = createSourceFromStaticConfig(env, params)
    log.info("Kinesis stream created.")
    val sourceetl = source.map(line => {
      try {
        val event: DataModel.Data = gson.fromJson(line, classOf[DataModel.Data])
        event
      } catch {
        case e: Exception => {
          log.error(e.getMessage)
        }
          DataModel.Data("error", "error")
      }
    }).filter(_.name != null)
    sourceetl.addSink(createSinkFromStaticConfig(params)).setParallelism(1)
    log.info("S3 Sink added.")
    env.execute("kinesis analytics from data stream to s3")
  }

  def createSourceFromStaticConfig(env: StreamExecutionEnvironment, params: ParamsModel.Params): DataStream[String] = {
    val consumerConfig = new Properties()
    consumerConfig.put(AWSConfigConstants.AWS_REGION, params.awsRgeion)
    // 生产环境不用AKSK，本地调试可以使用
    if (params.ak != null && params.sk != null) {
      consumerConfig.put(AWSConfigConstants.AWS_ACCESS_KEY_ID, params.ak)
      consumerConfig.put(AWSConfigConstants.AWS_SECRET_ACCESS_KEY, params.sk)
    }
    // 从哪个位置消费
    consumerConfig.put(ConsumerConfigConstants.STREAM_INITIAL_POSITION, params.streamInitPosition)
    if (params.streamInitPosition.equalsIgnoreCase(StreamPos.AT_TIMESTAMP.toString)) {
      consumerConfig.setProperty(ConsumerConfigConstants.STREAM_INITIAL_TIMESTAMP, params.streamInitialTimestamp)
    }

    val kinesis = env.addSource(new FlinkKinesisConsumer[String](
      params.inputStreamName, new SimpleStringSchema(), consumerConfig))
    kinesis
  }

  def createSinkFromStaticConfig(params: ParamsModel.Params) = {
    val sink = StreamingFileSink
      .forBulkFormat(
        new Path(params.s3OutputPath),
        ParquetAvroWriters.forReflectRecord(classOf[DataModel.Data])
      ).withBucketAssigner(new CustomBucketAssigner)
      .build()
    sink
  }

  def createSinkFromString(outpath: String) = {
    val sink = StreamingFileSink
      .forBulkFormat(
        new Path(outpath),
        ParquetAvroWriters.forReflectRecord(classOf[DataModel.Data])
      ).withBucketAssigner(new CustomBucketAssigner)
      .build()
    sink
  }

  def createRowSinkFromString(outpath: String)={
  val sink: StreamingFileSink[String] = StreamingFileSink
    .forRowFormat(new Path(outpath), new SimpleStringEncoder[String]("UTF-8"))
    .withRollingPolicy(
      DefaultRollingPolicy.builder()
        .withRolloverInterval(TimeUnit.MINUTES.toMillis(15))
        .withInactivityInterval(TimeUnit.MINUTES.toMillis(5))
        .withMaxPartSize(1024 * 1024 * 1024)
        .build())
    .build()
}

//  final StreamingFileSink<String> sink = StreamingFileSink
//    .forRowFormat(new Path(outputPath), new SimpleStringEncoder<String>("UTF-8"))
//    .withRollingPolicy(
//      DefaultRollingPolicy.builder()
//        .withRolloverInterval(TimeUnit.MINUTES.toMillis(15))
//        .withInactivityInterval(TimeUnit.MINUTES.toMillis(5))
//        .withMaxPartSize(1024 * 1024 * 1024)
//        .build())
//    .build();



}
