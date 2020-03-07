package org.kebonbinatang.kafka.connector.rest

/**
  * @constructor
  * @param properties is set of configurations required to create JdbcSourceTaskConfig
  */
class RestSourceTaskConfig(properties: Map[String, String]) extends RestSourceConnectorConfig(properties)
