package com.num3rd.spark.streaming

import kafka.api.OffsetRequest
import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import kafka.utils.{ZKStringSerializer, ZkUtils}
import org.I0Itec.zkclient.ZkClient
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.{SparkConf, TaskContext}
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils, OffsetRange}
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
    val ssc = new StreamingContext(sparkConf, Seconds(10))

    val kafkaParams = Map[String, String](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> brokers,
      ConsumerConfig.GROUP_ID_CONFIG -> groupId,
      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> OffsetRequest.SmallestTimeString
    )

    // /consumers/[groupId]/offsets/[topic]/[partitionId] -> long (offset)
    val zkPath = "/consumers/".concat(groupId) + "/offsets/".concat(topic)
    // ZKStringSerializer for serializer, Or will be unreadable
    val zkClient = new ZkClient(zks, 3000, 3000, ZKStringSerializer)

    val (offsetsRangesStrOpt, _) = ZkUtils.readDataMaybeNull(zkClient, zkPath)

    val zkOffsetData = offsetsRangesStrOpt match {
      case Some(offsetsRangesStr) =>
        var offsets = offsetsRangesStr.split(",")
          .map(s => s.split(":"))
          .map {
            case Array(partitionStr, offsetStr) => (TopicAndPartition(topic, partitionStr.toInt) -> offsetStr.toLong)
          }
          .toMap

        // If new partitions has been added
        val latestPartitions = ZkUtils.getPartitionsForTopics(zkClient, Seq(topic)).get(topic).get
        println(offsets + ":" + latestPartitions)
        if (offsets.size < latestPartitions.size) {
          val oldPartitions = offsets.keys.map(p => p.partition).toArray
          val newPartitions = latestPartitions.diff(oldPartitions)
          if (newPartitions.size > 0) {
            newPartitions.foreach(partitionId => {
              // Add new partitions
              offsets += (TopicAndPartition(topic, partitionId) -> 0)
            })
          }
        }
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

    messages.foreachRDD(rdd => {
      if (!rdd.isEmpty()) {
        val offset = rdd.asInstanceOf[HasOffsetRanges].offsetRanges.map(
          offsetRange => s"${offsetRange.partition}:${offsetRange.untilOffset}")
          .mkString(",")

        ZkUtils.updatePersistentPath(zkClient, zkPath, offset)
      }
    })

    messages.foreachRDD(rdd => {
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd.foreachPartition { partitions => {
        val o: OffsetRange = offsetRanges(TaskContext.get.partitionId)
        println(s"${o.topic} ${o.partition} ${o.fromOffset} ${o.untilOffset}")

        partitions.foreach(msg => {
          println(msg._2 + "===msg")
        })

      }
      }
    })

    ssc.start()
    ssc.awaitTermination()
  }

}
