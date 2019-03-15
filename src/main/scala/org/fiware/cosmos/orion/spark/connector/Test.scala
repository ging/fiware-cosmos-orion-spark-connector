package org.fiware.cosmos.orion.spark.connector

import java.io.{BufferedReader, InputStreamReader}
import java.net.Socket
import java.nio.charset.StandardCharsets
import scala.collection.Map

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.receiver.Receiver

import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.sql._
object Test {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName("CustomReceiver").setMaster("local")
    val ssc = new StreamingContext(sparkConf, Seconds(1))
    val customReceiverStream = ssc.receiverStream(new OrionReceiver("138.4.7.110", 9001))
    customReceiverStream.print()
    print("yes")
    ssc.start()
    ssc.awaitTermination()
  }
}