package org.fiware.cosmos.orion.spark.connector.tests

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.fiware.cosmos.orion.spark.connector._

object Test {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("CustomReceiver").setMaster("local[3]")
    val ssc = new StreamingContext(sparkConf, Seconds(1))
    val customReceiverStream : DStream[NgsiEvent] = ssc.receiverStream(new OrionReceiver("localhost", 9001))//host ="138.4.7.110"
    print("--------------------------¿¿???-------------------------")
    customReceiverStream.print

    val res : DStream[OrionSinkObject] = customReceiverStream
      .flatMap(event => event.entities)
      .map(s =>{
        OrionSinkObject("{'msg': 'hola'}","http://localhost:5000/fiware",ContentType.JSON, HTTPMethod.POST)
      })
    OrionSink.addSink(res)

    ssc.start()

    ssc.awaitTermination()

  }
}