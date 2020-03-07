package org.kebonbinatang.kafka.connector.rest

import java.util.{List => JavaList, Map => JavaMap}
import java.util.concurrent.atomic.{AtomicBoolean => JavaBoolean}

import org.kebonbinatang.kafka.connector.rest.source.{KafkaSourceService, TransporterHttpService}
import org.apache.kafka.connect.data.{Schema, Struct}
import org.apache.kafka.connect.errors.ConnectException
import org.apache.kafka.connect.source.{SourceRecord, SourceTask}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

class RestSourceTask extends SourceTask {
  private val taskLogger: Logger = LoggerFactory.getLogger(classOf[RestSourceTask])
  private var taskConfig: RestSourceTaskConfig  = _
  private var sourceService: KafkaSourceService[String, Struct] = _
  private var running: JavaBoolean = _

  override def version(): String = RestSourceVersion.getVersion

/**
  * invoked by kafka-connect runtime to start this task
  *
  * @param connectorProperties properties required to start this task
  */
  override def start(connectorProperties: JavaMap[String, String]): Unit = {
    Try(new RestSourceTaskConfig(connectorProperties.asScala.toMap)) match {
      case Success(cfg) => taskConfig = cfg
      case Failure(err) => taskLogger.error(s"====D~ Could not start Task ${this.getClass.getName} due to error in configuration.", new ConnectException(err))
    }

    val apiHttpUrl: String = taskConfig.getApiHttpUrl
    val apiKey: String = taskConfig.getApiKey
    val apiParams: Map[String, String] = taskConfig.getApiParams

    val pollInterval: Long = taskConfig.getPollInterval

    taskLogger.info(s"===============================================================================")
    taskLogger.info(s"====D~ Setting up an HTTP service for ${apiHttpUrl}...")
    Try( new TransporterHttpService(taskConfig.getTopic, taskConfig.getService, apiHttpUrl, apiKey, apiParams) ) match {
      case Success(service) =>  sourceService = service
      case Failure(error) =>    taskLogger.error(s"====D~ Could not establish an HTTP service to ${apiHttpUrl}")
                                throw error
    }

    taskLogger.info(s"================================================================================")
    taskLogger.info(s"====D~ Starting fetch ${apiHttpUrl} each ${pollInterval}ms...")
    running = new JavaBoolean(true)
  }

  /**
    * invoked by kafka-connect runtime to stop this task
    */
  override def stop(): Unit = this.synchronized {
    taskLogger.info("====D~ Stopping task.")
    running.set(false)
  }

  /**
    * invoked by kafka-connect runtime to poll data in [[HttpSourceConnectorConstants.POLL_INTERVAL_MS_CONFIG]] interval
    */
  override def poll(): JavaList[SourceRecord] = this.synchronized { if(running.get) fetchRecords else null }

  /**
    * invoked by kafka-connect runtime to get records from source system
    */
  private def fetchRecords: JavaList[SourceRecord] = {
    taskLogger.debug("=============================================================================")
    taskLogger.debug("====D~ Polling new data...")

    val pollInterval = taskConfig.getPollInterval
    val startTime    = System.currentTimeMillis

    val fetchedRecords: Seq[SourceRecord] = Try(sourceService.sourceRecords) match {
      case Success(records)                    => if(records.isEmpty) taskLogger.info(s"====D~ No data from ${taskConfig.getService}")
                                                  else taskLogger.info(s"====D~ Got ${records.size} results for ${taskConfig.getService}")
                                                  records

      case Failure(error: Throwable)           => taskLogger.error(s"====D~ Failed to fetch data for ${taskConfig.getService}: ", error)
                                                  Seq.empty[SourceRecord]
    }

    val endTime     = System.currentTimeMillis
    val elapsedTime = endTime - startTime

    if(elapsedTime < pollInterval) Thread.sleep(pollInterval - elapsedTime)

    fetchedRecords.asJava
  }

}
