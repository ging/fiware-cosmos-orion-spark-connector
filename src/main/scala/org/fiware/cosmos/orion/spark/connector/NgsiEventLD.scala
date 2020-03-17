package org.fiware.cosmos.orion.spark.connector

/**
  * NgsiEvent
  * @param creationTime Time the event was received (milliseconds since 1970)
  * @param fiwareService Fiware service header
  * @param fiwareServicePath Fiware service path header
  * @param entities List of entities
  */
case class NgsiEventLD(creationTime: Long, fiwareService: String ,fiwareServicePath: String, entities: Seq[EntityLD] ) extends Serializable

/**
  * Entity
  * @param id Identification of the entity
  * @param `type` Entity type
  * @param attrs List of attributes
  */
case class EntityLD(id: String,  `type`: String, attrs: Map[String, Map[String,Any]],context: Any) extends Serializable


