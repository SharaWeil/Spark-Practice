import java.io.InputStream
import java.util.Properties

import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.{KafkaCluster, KafkaUtils}

object MyKafkaUtil {
  private val properties = new Properties()

  def createKafkaDS(ssc: StreamingContext, propertyPath: String): InputDStream[String] = {
    val inputStream: InputStream = MyKafkaUtil.getClass.getClassLoader.getResourceAsStream(propertyPath)
    properties.load(inputStream)
    val kafkaBrokerList: String = properties.getProperty("bootstrap")
    val topic: String = properties.getProperty("kafka.topic")
    val topicSet = Set(topic)
    val kafkaGroup: String = properties.getProperty("kafka.group")
    val kafkaParams = Map {
      "bootstrap" -> kafkaBrokerList
      "group.id" -> kafkaGroup
    }
    val cluster = new KafkaCluster(kafkaParams)
    val fromOffsets: Map[TopicAndPartition, Long] = ZookeeperUtil.getOffsetFromZookeeper(cluster,topicSet,kafkaGroup)
    val kafkaDStream: InputDStream[String] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, String](ssc,
      kafkaParams,
      fromOffsets,
      (message: MessageAndMetadata[String, String]) => message.message()
    )
    kafkaDStream
  }
}
