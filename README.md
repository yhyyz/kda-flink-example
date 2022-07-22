### Kafka2Hive
#### KDA消费Kafka数据到Hive表
1. 使用HiveMetastore链接到Hive,获取元数据，可以链接EMR，也可以是自建集群，分区自动提交，小文件合并
2. 主函数Kafka2Hive
3. 写入的Hive表数据可以是S3也可以为HDFS,都支持
3. 样例数据
```json
# 样例数据
{"uuid":"999d0f4f-9d49-4ad0-9826-7a01600ed0b8","date":"2021-04-13T06:23:10.593Z","timestamp":1617171790593,"ad_type":1203,"ad_type_name":"udxyt"}
```
#### 本地调试参数,KDA Console参数与之相同，去掉参数前的-即可
```
-broker_list localhost:9092
-source_topic test_001
-consumer_group kda-test-g1
-kafka_tablename event_log
-hive_metastore thrift://localhost:9083
-hive_database default
-hive_table test_s3_01
-hive_table_path s3a://app-util/tmp/kda-test-01/ 或者 /hdfs/tmp/tmp/kda-test-01/(注意hdfs的链接信息在项目的resource目录中，打包时会打进去)

# KDA Params Group: FlinkTableAppProperties
```
![](https://pcmyp.oss-accelerate.aliyuncs.com/markdown/20220722205917.png)
![](https://pcmyp.oss-accelerate.aliyuncs.com/markdown/20220722210237.png)
![](https://pcmyp.oss-accelerate.aliyuncs.com/markdown/20220722210104.png)
![](https://pcmyp.oss-accelerate.aliyuncs.com/markdown/20220722210357.png)
### Streaming2S3 
#### KDA消费KDS(kinesis data streams)数据写入S3
1. 主函数Streaming2S3
2. 实现自定义Bucket，也就是自定义输入到S3的目录格式，抽取日志中的某个字段值作为分区目录

#### 本地调试参数
```
-aws_region 你的region -ak 你的AK -sk 你的SK
-input_stream_name KDA名字
-stream_init_position 消费位置
-s3_output_path 输出到S3目录

# 注意本地调试时，由于要输出到S3,需要在程序环境变量中加入
AWS_ACCESS_KEY_ID= 你的AK
AWS_SECRET_ACCESS_KEY= 你的SK
```
#### 部署到KDA参数
```
# 在KDA Console配置或者CLI都可以
Group: FlinkAppProperties
Key: aws_region
Value: 你的region
Key: input_stream_name
Value: KDA Steaming名字
Key: s3_output_path
Value: 输出路径
Key: stream_init_position
Value: 消费位置

```
#### build
```
 mvn clean package -Dscope.type=provided
```
