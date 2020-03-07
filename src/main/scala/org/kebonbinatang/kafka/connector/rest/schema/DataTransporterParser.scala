package org.kebonbinatang.kafka.connector.rest.schema

import org.kebonbinatang.kafka.connector.rest.RestSourceTask
import io.circe.Decoder
import io.circe.parser.decode

import io.circe.generic.auto._
//import io.circe.parser._

import org.apache.kafka.connect.data.{Schema, SchemaBuilder, Struct}
import org.slf4j.{Logger, LoggerFactory}

object DataTransporterParser extends KafkaSchemaParser[String, Struct] {

    private val logger: Logger = LoggerFactory.getLogger(classOf[RestSourceTask])

    override val schema: Schema = SchemaBuilder.struct().name("DataTransporterSchema")
        .field("idDataTransporterDetail", Schema.STRING_SCHEMA)
        .field("idRequestBooking", Schema.STRING_SCHEMA)
        .field("container_no", Schema.STRING_SCHEMA)
        .field("container_size", Schema.STRING_SCHEMA)
        .field("container_type", Schema.STRING_SCHEMA)
        .field("idEseal", Schema.STRING_SCHEMA)
        .field("truckPlateNo", Schema.STRING_SCHEMA)
        .field("namaDriver", Schema.STRING_SCHEMA)
        .field("hpDriver", Schema.STRING_SCHEMA)
        .field("urlTracking", Schema.STRING_SCHEMA)
        .field("isFinished", Schema.STRING_SCHEMA)
        .field("bl_no", Schema.STRING_SCHEMA)
        .field("bl_date", Schema.STRING_SCHEMA)
        .field("sp2valid_date", Schema.STRING_SCHEMA)
        .field("spcvalid_date", Schema.STRING_SCHEMA)
        .field("gross_weight", Schema.STRING_SCHEMA)
        .field("pod", Schema.STRING_SCHEMA)
        .field("pod_lat", Schema.STRING_SCHEMA)
        .field("pod_lon", Schema.STRING_SCHEMA)
        .field("destination", Schema.STRING_SCHEMA)
        .field("destination_lat", Schema.STRING_SCHEMA)
        .field("destination_lon", Schema.STRING_SCHEMA)
        .field("depo", Schema.STRING_SCHEMA)
        .field("depo_lat", Schema.STRING_SCHEMA)
        .field("depo_lon", Schema.STRING_SCHEMA)
        .field("total_distance", Schema.STRING_SCHEMA)
        .field("id_platform", Schema.STRING_SCHEMA)
        .field("nama_platform", Schema.STRING_SCHEMA)
        .field("company", Schema.STRING_SCHEMA)
        .field("billing_code", Schema.STRING_SCHEMA)
        .field("booked_date", Schema.STRING_SCHEMA)
        .field("logtruck-idLogTruck", Schema.STRING_SCHEMA)
        .field("logtruck-idDataTransporterDetil", Schema.STRING_SCHEMA)
        .field("logtruck-status", Schema.STRING_SCHEMA)
        .field("logtruck-create_date", Schema.STRING_SCHEMA)
        .field("status", Schema.STRING_SCHEMA)

        .build()


    private val emptyString: String = "N/A" // todo: optional data type???
    private val emptyNumber: Double = -1 // todo: optional data tyoe???

    case class Coord(lon: Double, lat: Double)

    case class LogTruck(idLogTruck: String, idDataTransporterDetil: String, status: String, create_date: String)

    case class Main(temp: Double, pressure: Double, humidity: Double, temp_min: Double, temp_max: Double)

    case class Wind(speed: Double, deg: Double)
    object Wind {
        implicit val decoder: Decoder[Wind] = Decoder.instance { h =>
            for {
                speed <- h.getOrElse[Double]("speed")(emptyNumber) // todo: optional schema type!!!!!
                deg <- h.getOrElse[Double]("deg")(emptyNumber)
            } yield Wind(
                speed,
                deg
            )
        }
    }

    case class Rain(threeH: Double)
    object Rain {
        implicit val decoder: Decoder[Rain] = Decoder.instance { h =>
            for {
                threeH <- h.get[Double]("3h")
            } yield Rain(
                threeH
            )
        }
    }

    case class Clouds(all: Double)

    case class Sys(sysType: Double, id: Double, message: Double, country: String, sunrise: Double, sunset: Double)
    object Sys {
        implicit val decoder: Decoder[Sys] = Decoder.instance { h =>
            for {
                sysType <- h.get[Double]("type")
                id <- h.get[Double]("id")
                message <- h.get[Double]("message")
                country <- h.get[String]("country")
                sunrise <- h.get[Double]("sunrise")
                sunset <- h.get[Double]("sunset")
            } yield Sys(
                sysType,
                id,
                message,
                country,
                sunrise,
                sunset
            )
        }
    }

    val emptyCoord = Coord(emptyNumber, emptyNumber)
    val emptySys = Sys(emptyNumber, emptyNumber, emptyNumber, emptyString, emptyNumber, emptyNumber)
    val emptyMain = Main(emptyNumber, emptyNumber, emptyNumber, emptyNumber, emptyNumber)
    val emptyWind = Wind(emptyNumber, emptyNumber)
    val emptyRain = Rain(emptyNumber)
    val emptyClouds = Clouds(emptyNumber)
    val emptyLogTruck = LogTruck(emptyString, emptyString, emptyString,emptyString)


    case class DataTransporterSchema(
                                idDataTransporterDetail: String,
                                idRequestBooking: String,
                                container_no: String,
                                container_size: String,
                                container_type: String,
                                idEseal: String,
                                truckPlateNo: String,
                                namaDriver: String,
                                hpDriver: String,
                                urlTracking: String,
                                isFinished: String,
                                bl_no: String,
                                bl_date: String,
                                sp2valid_date: String,
                                spcvalid_date: String,
                                gross_weight: String,
                                pod: String,
                                pod_lat: String,
                                pod_lon: String,
                                destination: String,
                                destination_lat: String,
                                destination_lon: String,
                                depo: String,
                                depo_lat: String,
                                depo_lon: String,
                                total_distance: String,
                                id_platform: String,
                                nama_platform: String,
                                company: String,
                                billing_code: String,
                                booked_date: String,
                                logtruck: LogTruck,
                                status: String

                            )

    object DataTransporterSchema {

        implicit val decoder: Decoder[DataTransporterSchema] = Decoder.instance { h =>

            for {
                idDataTransporterDetail <-  h.get[String]("idDataTransporterDetail")
                idRequestBooking <- h.get[String]("idRequestBooking")
                container_no <- h.get[String]("container_no")
                container_size <- h.get[String]("container_size")
                container_type <- h.get[String]("container_type")
                idEseal <- h.get[String]("idEseal")
                truckPlateNo <- h.get[String]("truckPlateNo")
                namaDriver <- h.get[String]("namaDriver")
                hpDriver <- h.get[String]("hpDriver")
                urlTracking <- h.get[String]("urlTracking")
                isFinished <- h.get[String]("isFinished")
                bl_no <- h.get[String]("bl_no")
                bl_date <- h.get[String]("bl_date")
                sp2valid_date <- h.get[String]("sp2valid_date")
                spcvalid_date <- h.get[String]("spcvalid_date")
                gross_weight <- h.get[String]("gross_weight")
                pod <- h.get[String]("pod")
                pod_lat <- h.get[String]("pod_lat")
                pod_lon <- h.get[String]("pod_lon")
                destination <- h.get[String]("destination")
                destination_lat <- h.get[String]("destination_lat")
                destination_lon <- h.get[String]("destination_lon")
                depo <- h.get[String]("depo")
                depo_lat <- h.get[String]("depo_lat")
                depo_lon <- h.get[String]("depo_lon")
                total_distance <- h.get[String]("total_distance")
                id_platform <- h.get[String]("id_platform")
                nama_platform <- h.get[String]("nama_platform")
                company <- h.get[String]("company")
                billing_code <- h.get[String]("billing_code")
                booked_date <- h.get[String]("booked_date")
                logtruck <- h.get[LogTruck]("logtruck")
                status <- h.get[String]("status")
                
            } yield DataTransporterSchema(
                idDataTransporterDetail,
                idRequestBooking,
                container_no,
                container_size,
                container_type,
                idEseal,
                truckPlateNo,
                namaDriver,
                hpDriver,
                urlTracking,
                isFinished,
                bl_no,
                bl_date,
                sp2valid_date,
                spcvalid_date,
                gross_weight,
                pod, pod_lat, pod_lon, destination, destination_lat, destination_lon,
                depo, depo_lat, depo_lon, total_distance, id_platform, nama_platform,
                company, billing_code, booked_date, logtruck, status
            )

        }

    }

    val emptyTransporterSchema: DataTransporterSchema = DataTransporterSchema(
        emptyString,
        emptyString,
        emptyString, //container_no
        emptyString, //container_size
        emptyString, //container_type
        emptyString, ///idEseal
        emptyString, //truckPlateNo
        emptyString, //namaDriver
        emptyString, //hpDriver
        emptyString, //urlTracking
        emptyString, //isFinished
        emptyString, //bl_no
        emptyString, //bl_date
        emptyString, //sp2valid_date
        emptyString, //spc
        emptyString, //gros
        emptyString, //pod
        emptyString, //pod_lat
        emptyString,
        emptyString, //dest
        emptyString,
        emptyString,
        emptyString, //depo
        emptyString,
        emptyString,
        emptyString, //total dis
        emptyString, //id-platfom
        emptyString, emptyString, emptyString, emptyString, 
        emptyLogTruck,
        emptyString        
    )


    override def output(structInput: String): Struct = {

        logger.info(s"====D~ Schema parser: JSON text to be parsed: ${structInput}")

        val transportParsed: DataTransporterSchema = decode[DataTransporterSchema](structInput) match {
            case Left(error) => {
                logger.error(s"====D~ JSON parser error: ${error}")
                emptyTransporterSchema
            }
            case Right(idDataTransporterDetail) => idDataTransporterDetail
        }

        logger.info(s"====D~ JSON parsed class content: ${transportParsed}")


        val transportStruct: Struct = new Struct(schema)
            .put("idDataTransporterDetail", transportParsed.idDataTransporterDetail)
            .put("idRequestBooking", transportParsed.idRequestBooking)
            .put("container_no", transportParsed.container_no)
            .put("container_size", transportParsed.container_size)
            .put("container_type", transportParsed.container_type)
            .put("idEseal", transportParsed.idEseal)
            .put("truckPlateNo",transportParsed.truckPlateNo)
            .put("namaDriver",transportParsed.namaDriver)
            .put("hpDriver",transportParsed.hpDriver)
            .put("urlTracking",transportParsed.urlTracking)
            .put("isFinished",transportParsed.isFinished)
            .put("bl_no",transportParsed.bl_no)
            .put("bl_date",transportParsed.bl_date)
            .put("sp2valid_date",transportParsed.sp2valid_date)
            .put("spcvalid_date",transportParsed.spcvalid_date)
            .put("gross_weight",transportParsed.gross_weight)
            .put("pod",transportParsed.pod)
            .put("pod_lat",transportParsed.pod_lat)
            .put("pod_lon",transportParsed.pod_lon)
            .put("destination",transportParsed.destination)
            .put("destination_lat",transportParsed.destination_lat)
            .put("destination_lon",transportParsed.destination_lon)
            .put("depo",transportParsed.depo)
            .put("depo_lat", transportParsed.depo_lat)
            .put("depo_lon", transportParsed.depo_lon)
            .put("total_distance",transportParsed.total_distance)
            .put("id_platform",transportParsed.id_platform)
            .put("nama_platform", transportParsed.nama_platform)
            .put("billing_code",transportParsed.billing_code)
            .put("booked_date",transportParsed.booked_date)
            .put("logtruck-idLogTruck", transportParsed.logtruck.idLogTruck)
            .put("logtruck-idDataTransporterDetil", transportParsed.logtruck.idDataTransporterDetil)
            .put("logtruck-status", transportParsed.logtruck.status)
            .put("logtruck-create_date", transportParsed.logtruck.create_date)
            .put("status",transportParsed.status)

        transportStruct
    }

}
