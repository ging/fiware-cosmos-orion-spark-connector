package org.fiware.cosmos.orion.spark.connector

import java.io.{BufferedReader, InputStreamReader}
import java.net.Socket
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.receiver.Receiver



class OrionReceiver(host: String, port: Int)
  extends Receiver[NgsiEvent](StorageLevel.MEMORY_AND_DISK_2) with Logging {

private var server: OrionHttpServer =_

  def receive(x: NgsiEvent): Unit ={
    store(x)
  }

  def onStart() = {
    server = new OrionHttpServer(receive)
    server.start(port, None)
  }

  // There is nothing much to do as the thread calling receive()
  // is designed to stop by itself if isStopped() returns false
  def onStop(): Unit = {
    server.close()
  }

}