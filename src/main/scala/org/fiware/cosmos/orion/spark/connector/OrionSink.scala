package org.fiware.cosmos.orion.spark.connector

import org.apache.spark.streaming.dstream.DStream
import org.apache.http.client.methods._
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.HttpClientBuilder
import org.slf4j.LoggerFactory


case class OrionSinkObject(content: String, url: String, contentType: ContentType.Value, method: HTTPMethod.Value, headers: Map[String,String]= (Map[String,String]()))



object ContentType extends Enumeration {
  type ContentType = Value
  val JSON = Value("application/json")
  val Plain = Value("text/plain")
  val None = null
}

/**
  * HTTP Method of the message
  */
object HTTPMethod extends Enumeration {
  type HTTPMethod = Value
  val POST = Value("HttpPost")
  val PUT = Value("HttpPut")
  val PATCH = Value("HttpPatch")
  val DELETE = Value("HttpDelete")
}


object OrionSink {
  private lazy val logger = LoggerFactory.getLogger(getClass)

  def addSink(rddStream: DStream[OrionSinkObject]): Unit = {

    rddStream.foreachRDD { rdd =>
      rdd.foreach { record =>
        process(record)
      }
    }
  }

  def process(msg: OrionSinkObject): CloseableHttpResponse = {

    val httpEntity : HttpRequestBase = createHttpMsg(msg)
    val client = HttpClientBuilder .create.build

    try {
      val response = client.execute(httpEntity)
      logger.info("POST to " + msg.url)
      return response
    } catch {
      case e: Exception => {
        logger.error(e.toString)
      }
      return null
    }
  }

  /**
    * Http object creator for sending the message
    * @param method HTTP Method
    * @param url Destination URL
    * @return HTTP object
    */
 def getMethod(method: HTTPMethod.Value, url: String): HttpRequestBase   = {
    method match {
      case HTTPMethod.POST => new HttpPost(url)
      case HTTPMethod.PUT => new HttpPut(url)
      case HTTPMethod.PATCH => new HttpPatch(url)
      case HTTPMethod.DELETE => new HttpDelete(url)
    }
  }

 def createHttpMsg(msg: OrionSinkObject) : HttpRequestBase= {
   val httpEntity = getMethod(msg.method, msg.url)

   msg.contentType match {
     case ContentType.None => ""
     case _ => httpEntity.setHeader("Content-type", msg.contentType.toString)
   }

   if(msg.headers.nonEmpty){
     msg.headers.foreach({case(k,v) => httpEntity.setHeader(k,v)})
   }
   if (httpEntity.isInstanceOf[HttpEntityEnclosingRequestBase]){
     httpEntity.asInstanceOf[HttpEntityEnclosingRequestBase].setEntity(new StringEntity(msg.content))
   }
   httpEntity
 }
}
