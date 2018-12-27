
import java.util.Properties

import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.{KafkaCluster, KafkaUtils}
import org.apache.spark.streaming.{Duration, Seconds, StreamingContext}

object Test {
  def main(args: Array[String]): Unit = {
    val checkPointPath = "/testoldAPI"
    val sc: StreamingContext = StreamingContext.getActiveOrCreate(checkPointPath,createScFun())
    sc.start()
    sc.awaitTermination()
  }
  def createScFun(): () => StreamingContext ={
    ()=> {
      //创建streamingContext对象
      val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkOnline")
      //设置Spark应用程序优雅的关毕
      sparkConf.set("spark.streaming.stopGracefullyOnShutdown","true")
      //设置kafka每秒钟从kafka一个分区读取message的最大数量
      sparkConf.set("spark.streaming.kafka.maxRatePerPartition","100")
      val streamingContext = new StreamingContext(sparkConf, Seconds(5))
      streamingContext.checkpoint("/testoldAPI2")
      //设置kafka配置信息
      val properties: Properties = PropertiesUtils.getProperties("kafkaconf.properties")
      val kafkaBrokerList: String = properties.getProperty("bootstrap.servers")
      println(kafkaBrokerList)
      val topic: String = properties.getProperty("kafka.topic")
      val topicSet = Set(topic)
      val kafkaGroup: String = properties.getProperty("kafka.group")
      val kafkaParams = Map (
        "metadata.broker.list" -> kafkaBrokerList,
        "group.id" -> kafkaGroup
      )
      val cluster = new KafkaCluster(kafkaParams)
      val partitionAndOffset: Map[TopicAndPartition, Long] = ZookeeperUtil.getOffsetFromZookeeper(cluster, topicSet, kafkaGroup)
      //通过KafkaUtils获取DS
      val onlineDStream: InputDStream[String] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, String](
        streamingContext,
        kafkaParams,
        partitionAndOffset,
        (mess: MessageAndMetadata[String, String]) => mess.message()
      )
      onlineDStream.map(x=>(x,1)).updateStateByKey((values:Seq[Int],state:Option[Int]) =>{
        val sum: Int = values.sum
        val stateCount: Int = state.getOrElse(0)
        Some(sum+stateCount)
      }).print()
      ZookeeperUtil.writeOffsetToZookeeper(onlineDStream,cluster,kafkaGroup)
      streamingContext
    }
  }
}
