package org.kebonbinatang.kafka.connector.rest.source

//import org.kebonbinatang.kafka.connector.http.schema.{KafkaSchemaParser, TransporterSchemaParser}
import org.kebonbinatang.kafka.connector.rest.schema.{KafkaSchemaParser, DataTransporterParser}
import org.kebonbinatang.kafka.connector.rest.{RestSourceConnectorConstants, RestSourceTask}
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.source.SourceRecord
import org.slf4j.{Logger, LoggerFactory}
import scalaj.http.{Http, HttpRequest, HttpResponse}

import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

// caller is expected to handle exceptions
class TransporterHttpService(val topic: String, serviceName: String, apiBaseUrl: String, apiKey: String, apiParams: Map[String, String]) extends KafkaSourceService[String, Struct] {
//    val schemaParser: KafkaSchemaParser[String, Struct] = TransporterSchemaParser
    val schemaParser: KafkaSchemaParser[String, Struct] = DataTransporterParser

    private val logger: Logger = LoggerFactory.getLogger(classOf[RestSourceTask])

    override def sourceRecords: Seq[SourceRecord] = {

        //val sourceRecords: ListBuffer[SourceRecord] = ListBuffer.empty[SourceRecord]
        val transportResult: HttpResponse[String] = httpServiceResponse

        logger.info(s"====D~ Http return code: ${transportResult.code}")

        val record: Struct = schemaParser.output(transportResult.body)

        /*
        sourceRecords += new SourceRecord(
            Map(HttpSourceConnectorConstants.SERVICE_CONFIG -> serviceName).asJava, // partition
            Map("offset" -> "n/a").asJava, // offset
            topic,
            schemaParser.schema,
            record
        )

        sourceRecords
        */

        List(
            new SourceRecord(
                Map(RestSourceConnectorConstants.SERVICE_CONFIG -> serviceName).asJava, // partition
                Map("====D~ offset" -> "n/a").asJava, // offset
                topic,
                schemaParser.schema,
                record
            )
        )
    }

    private def httpServiceResponse: HttpResponse[String] = {

        @tailrec
        def addRequestParam(accu: HttpRequest, paramsToAdd: List[(String, String)]): HttpRequest = paramsToAdd match {
            case (paramKey,paramVal) :: rest => addRequestParam(accu.param(paramKey, paramVal), rest)
            case Nil => accu
        }

        val baseRequest = Http(apiBaseUrl).param("APPID",apiKey)
        val request = addRequestParam(baseRequest, apiParams.toList)

        request.asString
    }
}
