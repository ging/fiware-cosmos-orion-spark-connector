package org.fiware.cosmos.orion.spark.connector

import org.apache.spark.internal.Logging
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver



class NGSILDReceiver(port: Int)
  extends Receiver[NgsiEventLD](StorageLevel.MEMORY_AND_DISK_2) with Logging {

  private var server: OrionHttpServerLD =_

  def onStart() = {
    server = new OrionHttpServerLD(store)
    server.start(port, None)
  }

  def onStop(): Unit = {
    server.close()
  }

}