package com.num3rd.spark.streaming

import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import kafka.utils.ZkUtils
import org.I0Itec.zkclient.ZkClient
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object DirectKafkaWordCountOffset {
  def main(args: Array[String]): Unit = {
    if (args.length < 4) {
      System.err.println(
        """
          |Usage: DirectKafkaWordCount <brokers> <topics>
          |  <brokers> is a list of one or more Kafka brokers
          |  <topics> is a list of one or more kafka topics to consume from
          |  <zks> is a list of one or more zks
          |
        """.stripMargin)
      System.exit(1)
    }

    val Array(brokers, groupId, topic, zks) = args

    val sparkConf = new SparkConf().setAppName("DirectKafkaWordCountOffset")
    val ssc = new StreamingContext(sparkConf, Seconds(2))

    val kafkaParams = Map[String, String](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> brokers,
      ConsumerConfig.GROUP_ID_CONFIG -> groupId,
      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "smallest"
    )

    // /consumers/[groupId]/offsets/[topic]/[partitionId] -> long (offset)
    val zkPath = "/consumers/".concat(groupId) + "/offsets/".concat(topic)
    val zkClient = new ZkClient(zks)

    val (offsetsRangesStrOpt, _) = ZkUtils.readDataMaybeNull(zkClient, zkPath)

    val zkOffsetData = offsetsRangesStrOpt match {
      case Some(offsetsRangesStr) =>
        val offsets = offsetsRangesStr.split(",")
          .map(s => s.split(":"))
          .map {
            case Array(partitionStr, offsetStr) => (TopicAndPartition(topic, partitionStr.toInt) -> offsetStr.toLong)
          }
          .toMap
        Some(offsets)
      case None =>
        None
    }

    val messages = zkOffsetData match {
      case None =>
        KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, Set(topic))
      case Some(fromOffsets) =>
        KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, String)](
          ssc,
          kafkaParams,
          fromOffsets,
          (msg: MessageAndMetadata[String, String]) => msg.key() -> msg.message()
        )
    }

    val lines = messages.map(_._2)
    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x, 1L)).reduceByKey(_ + _)
    wordCounts.print()

    messages.foreachRDD(foreachFunc = rdd => {
      if (!rdd.isEmpty()) {
        val offset = rdd.asInstanceOf[HasOffsetRanges].offsetRanges.map(
          offsetRange => s"${offsetRange.partition}:${offsetRange.untilOffset}")
          .mkString(",")

        ZkUtils.updatePersistentPath(zkClient, zkPath, offset)
      }
    })

    ssc.start()
    ssc.awaitTermination()
  }

}
