package org.fiware.cosmos.orion.spark.connector

object Test{

  def main(args: Array[String]): Unit = {
    val customReceiverStream = ssc.receiverStream(new CustomReceiver(host, port))
  }