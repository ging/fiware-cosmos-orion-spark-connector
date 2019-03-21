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
  extends Receiver[String](StorageLevel.MEMORY_AND_DISK_2) with Logging {

private var server: OrionHttpServer =_

def store1(x: String): Unit ={
  store(x)
}

  def onStart() = {
    // Start the thread that receives data over a connection
    new Thread("Socket Receiver") {
      override def run() { receive() }
    }.start()
  }

  // There is nothing much to do as the thread calling receive()
  // is designed to stop by itself if isStopped() returns false
  def onStop(): Unit = {
    server.close()
  }



  /** Create a socket connection and receive data until receiver is stopped */
private def receive(): Unit = {

  server = new OrionHttpServer(store1)
  server.start(port, None)

   /*   var socket: Socket = null
    var userInput: String = null
    try {
      // Connect to host:port
      socket = new Socket(host, port)

      // Until stopped or connection broken continue reading
      val reader = new BufferedReader(
        new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8))
      userInput = reader.readLine()
      while(!isStopped && userInput != null) {
        store(userInput)
        userInput = reader.readLine()
      }
      reader.close()
      socket.close()

      // Restart in an attempt to connect again when server is active again
      restart("Trying to connect again")
    } catch {
      case e: java.net.ConnectException =>
        // restart if could not connect to server
        restart("Error connecting to " + host + ":" + port, e)
      case t: Throwable =>
        // restart if there is any other error
        restart("Error receiving data", t)
    }*/
  }

}