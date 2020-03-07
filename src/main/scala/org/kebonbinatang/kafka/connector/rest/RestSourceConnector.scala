package org.kebonbinatang.kafka.connector.rest

import java.util.{List => JavaList, Map => JavaMap}

import org.kebonbinatang.kafka.connector.rest.RestSourceConnectorConstants.{API_KEY_CONFIG, API_PARAMS_CONFIG}
import org.slf4j.LoggerFactory
import org.apache.kafka.connect.errors.ConnectException
import org.apache.kafka.connect.source.{SourceConnector, SourceTask}
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.common.config.ConfigDef.Importance
import org.apache.kafka.common.config.ConfigDef.Type

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}


class RestSourceConnector extends SourceConnector {
  private val connectorLogger = LoggerFactory.getLogger(classOf[RestSourceConnector])
  private var connectorConfig: RestSourceConnectorConfig = _

  private val configDef: ConfigDef =
      new ConfigDef()
          .define(RestSourceConnectorConstants.HTTP_URL_CONFIG, Type.STRING, Importance.HIGH, "Web API Access URL")
          .define(RestSourceConnectorConstants.API_KEY_CONFIG, Type.STRING, Importance.HIGH, "Web API Access Key")
          .define(RestSourceConnectorConstants.API_PARAMS_CONFIG, Type.STRING, Importance.HIGH, "Web API additional config parameters")
          .define(RestSourceConnectorConstants.SERVICE_CONFIG, Type.STRING, Importance.HIGH, "Kafka Service name")
          .define(RestSourceConnectorConstants.TOPIC_CONFIG, Type.STRING, Importance.HIGH, "Kafka Topic name")
          .define(RestSourceConnectorConstants.POLL_INTERVAL_MS_CONFIG, Type.STRING, Importance.HIGH, "Polling interval in milliseconds")
          .define(RestSourceConnectorConstants.TASKS_MAX_CONFIG, Type.INT, Importance.HIGH, "Kafka Connector Max Tasks")
          .define(RestSourceConnectorConstants.CONNECTOR_CLASS, Type.STRING, Importance.HIGH, "Kafka Connector Class Name (full class path)")

  override def config: ConfigDef = configDef

/**
  * @return the version of this connector
  */
  override def version: String = RestSourceVersion.getVersion

/**
  * invoked by Kafka connect runtime to start this connector
  *
  * @param connectorProperties properties required to start this connector
  */
  override def start(connectorProperties: JavaMap[String, String]): Unit = {
    Try (new RestSourceConnectorConfig(connectorProperties.asScala.toMap)) match {
      case Success(cfg) => connectorConfig = cfg
      case Failure(err) => connectorLogger.error(s"====D~ Could not start Kafka Source Connector ${this.getClass.getName} due to error in configuration.", new ConnectException(err))
    }
  }

/**
  * invoked by Kafka connect runtime to stop this connector
  */
  override def stop(): Unit = {
    connectorLogger.info(s"=============================================================================")
    connectorLogger.info(s"====D~ Stopping Kafka Source Connector ${this.getClass.getName}.")
  }

/**
  * invoked by Kafka connect runtime to instantiate SourceTask which polls data from external data store and saves into Kafka
  *
  * @return class of source task to be created
  */
  override def taskClass(): Class[_ <: SourceTask] = classOf[RestSourceTask]

/**
  * returns a set of configurations for tasks based on the current configuration
  *
  * @param maxTasks maximum number of configurations to generate
  * @return configurations for tasks
  */
  override def taskConfigs(maxTasks: Int): JavaList[JavaMap[String, String]] = List(connectorConfig.connectorProperties.asJava).asJava // Only returns one element in list. This assumes maxTasks = 1.

}
