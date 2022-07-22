package com.aws.analytics.kda

import com.aws.analytics.kda.model.DataModel
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.BasePathBucketAssigner

/**自定义Bucket，安装原始日志的name字段值分区，可以按照业务规则自己写逻辑
 */
class CustomBucketAssigner extends BasePathBucketAssigner[DataModel.Data] {
  override def getBucketId(element: DataModel.Data, context: BucketAssigner.Context): String = s"name=${element.name}"
}
