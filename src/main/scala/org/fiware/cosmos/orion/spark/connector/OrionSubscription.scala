  package org.fiware.cosmos.orion.spark.connector
  import org.apache.http.util.EntityUtils
  import org.fiware.cosmos.orion.spark.connector.OrionSubscriptionHelpers.{getClass, logger}
  import org.json4s.DefaultFormats
  import org.json4s.jackson.JsonMethods.parse
  import org.slf4j.LoggerFactory

  class OrionSubscription(entities: List[String], cbUrl: String, description: String, callbackHost: String, callbackPort: Int, content: String = ""  ) {
    implicit val formats = DefaultFormats
    private lazy val logger = LoggerFactory.getLogger(getClass)
    var subscriptionId = ""
    def this(entities: List[String], cbUrl: String, description: String, callbackHost: String, callbackPort: Int) {
      this(entities,
        OrionSubscriptionHelpers.getOrionHost(cbUrl),
        description,
        OrionSubscriptionHelpers.getCallbackHost(callbackHost, true),
        callbackPort,
        OrionSubscriptionHelpers.createContent(entities, description, callbackHost, callbackPort))
    }

    def subscribe() = {
      logger.info("Sending subscription to : " + cbUrl + "/v2/subscriptions" + "\n " + content)
      try {
        val response = OrionSink.process(OrionSinkObject(content, cbUrl + "/v2/subscriptions", ContentType.JSON, HTTPMethod.POST))
        val responseString = EntityUtils.toString(response.getEntity())
        logger.info(response.getStatusLine().toString)
        subscriptionId = response.getHeaders("Location")(0).getValue.replace("/v2/subscriptions/","")
        logger.info("SubscriptionId: " + subscriptionId)
        if(responseString.nonEmpty) {
          logger.info("Response:" + responseString)
        }
      } catch {
        case e : Any => {
          logger.error(e.getMessage())
        }
      }
    }

    def unsubscribe() = {
      logger.info("Sending unsubscription to " + cbUrl + "/v2/subscriptions/" + subscriptionId)

      val response  = OrionSink.process(OrionSinkObject("", cbUrl + "/v2/subscriptions/" + subscriptionId, ContentType.None, HTTPMethod.DELETE))
      logger.info(response.getStatusLine().toString)
    }
  }


  object OrionSubscriptionHelpers {
    private lazy val logger = LoggerFactory.getLogger(getClass)

    def getCallbackHost(callbackHost : String, log: Boolean = false): String = {
      if (callbackHost.isEmpty) {
        val host = java.net.InetAddress.getLocalHost().getCanonicalHostName()
        val NOTIFICATION_HOST = scala.util.Properties.envOrElse("NOTIFICATION_HOST", host)
        if (NOTIFICATION_HOST == host) {
          if (log) logger.warn("Callback host not specified nor found in env variable NOTIFICATION_HOST. Using " + NOTIFICATION_HOST)
        } else {
          if (log) logger.info("Callback host not specified. Using NOTIFICATION_HOST " + NOTIFICATION_HOST)
        }
        return NOTIFICATION_HOST
      }
      if (log) logger.info("Callback host: " + callbackHost)
      callbackHost
    }

    def getOrionHost(orionHost : String, log: Boolean = false): String = {
      if (orionHost.isEmpty) {
        val ORION_HOST = scala.util.Properties.envOrElse("ORION_HOST", "localhost")
        logger.info("Orion: " + ORION_HOST)
        return ORION_HOST
      }
      logger.info("Orion: " + orionHost)
      orionHost
    }

    def createContent( entities: List[String],
                      description: String,
                      callbackHost: String,
                      callbackPort: Int): String = {

      val notifyUrl = OrionSubscriptionHelpers.getCallbackHost(callbackHost)
      val entityList = entities.map(entity => s"""{"id": "${entity}"}""").mkString(",")
      val str = s"""
        {
          "description": "${description}",
          "subject": {
            "entities": [${entityList}],
            "condition": {
              "attrs": []
            }
          },
          "notification": {
            "http": { "url": "${notifyUrl}:${callbackPort}" },
            "attrs": []
          }
        }
      """
      str
    }
  }
