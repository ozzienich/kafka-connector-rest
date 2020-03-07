package org.kebonbinatang.kafka.connector.rest.source

import org.kebonbinatang.kafka.connector.rest.schema.KafkaSchemaParser
import org.apache.kafka.connect.source.SourceRecord

trait KafkaSourceService[SchemaInputType, SchemaOutputType] {
    def sourceRecords: Seq[SourceRecord]
    val topic: String
    val schemaParser: KafkaSchemaParser[SchemaInputType, SchemaOutputType]
}
