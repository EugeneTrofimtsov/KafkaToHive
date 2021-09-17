package kafkatohive.target

import java.sql.Timestamp
import java.time.Instant

import sys.process._
import net.liftweb.json._
import org.apache.log4j.Logger
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}

import scala.collection.JavaConverters._
import kafkatohive.Parameters
import kafkatohive.source.Entity._
import kafkatohive.target.Entity._
import kafkatohive.target.IntegrityControl._

import scala.collection.mutable.ListBuffer

object ConsumerReader {

  def resetOffsetsCommand(topic: String)(implicit conf: Parameters): Unit = {
    val server = s"--bootstrap-server ${conf.BOOTSTRAP_SERVERS}"
    val group = s"--group ${conf.GROUP_ID}"
    val name = s"--topic $topic"
    val main = "--reset-offsets --to-earliest --execute"
    s"${conf.KAFKA_BIN_PATH}kafka-consumer-groups${conf.SCRIPT_ENDING} $server $group $name $main".!!
  }

  def loadFromKafka(topic: String)(implicit conf: Parameters, logger: Logger): Seq[String] = {
    logger.info(s"Started reading from kafka topic '$topic'...")
    val consumer = new KafkaConsumer[String, String](conf.consumerConfig)
    val messages: ListBuffer[String] = new ListBuffer[String]()
    var zeroAnswersNumber = 1
    try {
      consumer.subscribe(List(topic).asJava)
      if (conf.RESET_OFFSET_FLAG) resetOffsetsCommand(topic)
      while (zeroAnswersNumber != 0) {
        val response = consumer.poll(conf.POLL_DURATION).asScala
        if (response.isEmpty) zeroAnswersNumber -= 1
        else for (message <- response) {
          messages.append(message.value())
          logger.info(s"Topic: ${message.topic()}, Key: ${message.key()}, Value: ${message.value()}, " +
            s"Offset: ${message.offset()}, Partition: ${message.partition()}")
        }
      }
    } catch {
      case e: Exception => logger.error(e.getMessage)
    } finally {
      consumer.close()
    }
    logger.info(s"Kafka reading from topic '$topic' complete...")
    messages
  }

  def parseJson[T](jsonString: String)(implicit m: Manifest[T]): T = {
    implicit val formats: DefaultFormats.type = DefaultFormats
    val jsonValue = parse(jsonString)
    jsonValue.extract[T]
  }

  def formClientTable(data: Seq[String], schema: List[StructField])(implicit spark: SparkSession): DataFrame = {
    spark.createDataFrame(spark.sparkContext.parallelize(
      data.map(parseJson[Information]).map {
        elem =>
          val hashctrl = elem.msg.getHash(elem.dt)
          Row(
            new Timestamp(elem.dt.toLong * 1000L),
            new Timestamp(Instant.now.getEpochSecond * 1000L),
            elem.msg.lastname + " " + elem.msg.firstname + " " + elem.msg.patronymic.getOrElse(""),
            elem.msg.birthday.getOrElse(""),
            Seq(elem.msg.address.city, elem.msg.address.code, elem.msg.address.zipcode.getOrElse(""),
              elem.msg.address.street, elem.msg.address.house, elem.msg.address.apartment.getOrElse(""),
              elem.msg.address.status).mkString(" "),
            elem.msg.phone.getOrElse(Array.empty).length.toByte,
            elem.hashmsg,
            hashctrl,
            elem.hashmsg == hashctrl
          )
      }
    ), StructType(schema))
  }

  def formClientPhoneTable(data: Seq[String], schema: List[StructField])(implicit spark: SparkSession): DataFrame = {
    spark.createDataFrame(spark.sparkContext.parallelize(
      data.map(parseJson[Information]).flatMap {
        elem =>
          for (phone <- elem.msg.phone.getOrElse(Array.empty)) yield
            Row(
              new Timestamp(elem.dt.toLong * 1000L),
              new Timestamp(Instant.now.getEpochSecond * 1000L),
              elem.msg.lastname + " " + elem.msg.firstname + " " + elem.msg.patronymic.getOrElse(""),
              phone
            )
      }
    ), StructType(schema))
  }

  def formControlTable(data: Seq[String], schema: List[StructField])(implicit spark: SparkSession): DataFrame = {
    spark.createDataFrame(spark.sparkContext.parallelize(
      data.map(parseJson[Control]).map(
        elem =>
          Row(
            new Timestamp(elem.dt.toLong * 1000L),
            new Timestamp(Instant.now.getEpochSecond * 1000L),
            elem.cnt.toLong
          )
      )
    ), StructType(schema))
  }

  def writeToHive(data: DataFrame, fullname: String)
                 (implicit spark: SparkSession, conf: Parameters, logger: Logger): Unit = {
    logger.info(s"Started writing table $fullname to hive...")
    conf.LOAD_TYPE match {
      case "incremental" =>
        data.write.format(conf.FORMAT_STORAGE).mode(SaveMode.Append).saveAsTable(fullname)
      case "overwrite" =>
        spark.sql(s"DROP TABLE IF EXISTS $fullname")
        data.write.format(conf.FORMAT_STORAGE).mode(SaveMode.ErrorIfExists).saveAsTable(fullname)
      case "test" =>
        data.show(false)
    }
    logger.info(s"Hive writing table $fullname complete...")
  }

  def formFullName(tableName: String)(implicit conf: Parameters): String = {
    s"${conf.SCHEMA}.$tableName${if (conf.DISABLE_CHECKS) "" else "_tmp"}"
  }

  def main(args: Array[String]): Unit = {
    implicit val logger: Logger = Logger.getLogger(this.getClass.getName)
    implicit val conf: Parameters = new Parameters(args)
    implicit val spark: SparkSession = SparkSession.builder.enableHiveSupport().getOrCreate()
    val stagingTables = conf.TOPICS_TO_LOAD match {
      case "all" =>
        val values = loadFromKafka(conf.MAIN_TOPIC)
        writeToHive(formClientTable(values, clientSchema), formFullName(conf.CLIENT_TABLE))
        writeToHive(formClientPhoneTable(values, clientPhoneSchema), formFullName(conf.CLIENT_PHONE_TABLE))
        val controlValues = loadFromKafka(conf.LOG_TOPIC)
        writeToHive(formControlTable(controlValues, controlSchema), formFullName(conf.LOG_TABLE))
        Seq(conf.CLIENT_TABLE, conf.CLIENT_PHONE_TABLE, conf.LOG_TABLE)
      case topicName if topicName == conf.MAIN_TOPIC =>
        val values = loadFromKafka(conf.MAIN_TOPIC)
        writeToHive(formClientTable(values, clientSchema), formFullName(conf.CLIENT_TABLE))
        writeToHive(formClientPhoneTable(values, clientPhoneSchema), formFullName(conf.CLIENT_PHONE_TABLE))
        Seq(conf.CLIENT_TABLE, conf.CLIENT_PHONE_TABLE)
      case topicName if topicName == conf.LOG_TOPIC =>
        val controlValues = loadFromKafka(conf.LOG_TOPIC)
        writeToHive(formControlTable(controlValues, controlSchema), formFullName(conf.LOG_TABLE))
        Seq(conf.LOG_TABLE)
      case _ =>
        logger.error(s"No such topic '${conf.TOPICS_TO_LOAD}'")
        Seq()
    }
    if (conf.LOAD_TYPE != "test" && !conf.DISABLE_CHECKS) for (tableName <- stagingTables) {
      val info = spark.sql(s"select * from ${conf.SCHEMA}.${tableName}_tmp")
      if (checkRecordsUnique(info)) logger.info(s"Integrity control passed for table $tableName: no duplicates")
      else logger.warn(s"Integrity control fails for table $tableName: duplicates found")
      if (info.columns.contains("verify")) {
        val num = countColumnValue(info, "verify", false)
        if (num == 0) logger.info(s"Integrity control passed for table $tableName: 0 wrong values")
        else logger.warn(s"Integrity control fails for table $tableName: $num wrong values")
      }
      writeToHive(info, s"${conf.SCHEMA}$tableName")
      spark.sql(s"DROP TABLE IF EXISTS ${conf.SCHEMA}.${tableName}_tmp")
    }
  }
}
