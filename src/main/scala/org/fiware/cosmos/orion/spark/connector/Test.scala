package org.fiware.cosmos.orion.spark.connector

import java.io.{BufferedReader, InputStreamReader}
import java.net.Socket
import java.nio.charset.StandardCharsets
import scala.collection.Map

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.receiver.Receiver

import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.sql._

object Test {

  def main(args: Array[String]): Unit = {
    print("yes")
    val sparkConf = new SparkConf().setAppName("CustomReceiver").setMaster("local[5]")
    val ssc = new StreamingContext(sparkConf, Seconds(1))
    val customReceiverStream = ssc.receiverStream(new OrionReceiver("localhost", 9001))//host ="138.4.7.110"
    print("---------------------------------------------------")
    customReceiverStream.print


//    customReceiverStream.foreachRDD(a=>a.foreach(b=>OrionSender.process( OrionSenderObject("", "",ContentType.Plain, HTTPMethod.PATCH)))
   // )


    ssc.start()

    ssc.awaitTermination()
  }
}