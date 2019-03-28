package org.fiware.cosmos.orion.spark.connector

import org.apache.spark.internal.Logging
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver



class OrionReceiver(host: String, port: Int)
  extends Receiver[NgsiEvent](StorageLevel.MEMORY_AND_DISK_2) with Logging {

private var server: OrionHttpServer =_

  def onStart() = {
    server = new OrionHttpServer(store)
    server.start(port, None)
  }

  def onStop(): Unit = {
    server.close()
  }

}