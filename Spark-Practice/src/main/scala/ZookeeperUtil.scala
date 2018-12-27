import kafka.common.TopicAndPartition
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaCluster, OffsetRange}
import org.apache.spark.streaming.kafka.KafkaCluster.Err

import scala.collection.mutable

object ZookeeperUtil {
  /**
    * 手动提交offset
    * @param onlineDStream 消费者流
    * @param cluster  kafka集群
    * @param group 消费者组
    */
  def writeOffsetToZookeeper(onlineDStream: InputDStream[String], cluster: KafkaCluster, group: String): Unit ={
    onlineDStream.foreachRDD{
      rdd =>
        //获取每个rdd中的offset信息
        val offsetList: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        //遍历每一个offset信息，更新并保存在zookeeper中
        for (offsetRange <- offsetList) {
          val topicAndPartition = TopicAndPartition(offsetRange.topic,offsetRange.partition)
          val ack: Either[Err, Map[TopicAndPartition, Short]] = cluster.setConsumerOffsets(group,Map((topicAndPartition,offsetRange.untilOffset)))
          if(ack.isLeft){
            println(s"Error updating the offset to Kafka cluster: ${ack.left.get}")
          }else{
            println(s"update the offset to Kafka cluster: ${offsetRange.untilOffset} successfully")
          }
        }
    }
  }
  /**
    * 手动提交offset
    * @param cluster 集群id
    * @param topics 消费主题
    * @param groupId 消费者组id
    * @return
    */
  def getOffsetFromZookeeper(cluster: KafkaCluster, topics: Set[String], groupId: String): Map[TopicAndPartition, Long] = {
    val topicAndPartitionMap = new mutable.HashMap[TopicAndPartition, Long]()
    // 获取传入的Topic的所有分区
    val topicAndPartitions: Either[Err, Set[TopicAndPartition]] = cluster.getPartitions(topics)
    // 如果成功获取到Topic所有分区
    if (topicAndPartitions.isRight) {
      // 获取分区数据
      val partitionList: Set[TopicAndPartition] = topicAndPartitions.right.get
      // 获取指定分区的offset
      val offsetInfo: Either[Err, Map[TopicAndPartition, Long]] = cluster.getConsumerOffsets(groupId, partitionList)
      // 如果指定分区没有offset信息则存储，则设为0
      if (offsetInfo.isLeft) {
        for (partition <- partitionList) {
          topicAndPartitionMap += (partition -> 0)
        }
      } else {
        // 如果有offset信息则获取offset
        val offsetMap: Map[TopicAndPartition, Long] = offsetInfo.right.get
        for ((topicAndPartiton, offset) <- offsetMap) {
          topicAndPartitionMap += (topicAndPartiton -> offset)
        }
      }
    }
    topicAndPartitionMap.toMap
  }

}
