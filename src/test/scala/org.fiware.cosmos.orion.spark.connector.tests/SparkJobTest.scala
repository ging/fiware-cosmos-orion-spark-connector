package org.fiware.cosmos.orion.spark.connector.test

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.fiware.cosmos.orion.spark.connector._


object Constants {
  final val Port = 9102
}

/**
  * Example1 Orion Connector
  * @author @sonsoleslp
  */
object SparkJobTest{

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("CustomReceiver").setMaster("local[3]")
    val ssc = new StreamingContext(sparkConf, Seconds(10))

    // Create Orion Source. Receive notifications on port 9001
    val eventStream : DStream[NgsiEvent] = ssc.receiverStream(new OrionReceiver("localhost", Constants.Port))

    // Process event stream
      val processedDataStream = eventStream
      .flatMap(event => event.entities)
      .map(entity => {
        val temp = entity.attrs("temperature").value.asInstanceOf[Number].floatValue()
        val pres = entity.attrs("pressure").value.asInstanceOf[Number].floatValue()
        ( entity.id , ( temp, pres) )
      })
      .reduceByKeyAndWindow((a,b)=> {(a._1 max b._1, a._2 max b._2)},Seconds(10))
      .map(x => {
        simulatedNotification.maxTempVal = x._2._1
        simulatedNotification.maxPresVal = x._2._2
        OrionSinkObject("{'msg': 'hola'}","http://localhost:5000/fiware",ContentType.JSON, HTTPMethod.POST)
      })
    processedDataStream.print()
    OrionSink.addSink(processedDataStream)


    ssc.start()
    Thread.sleep(50000)
    ssc.stop()
  }

  case class EntityNode(id: String, temperature: Float, pressure: Float)
}
