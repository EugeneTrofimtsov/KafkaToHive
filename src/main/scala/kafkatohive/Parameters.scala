package kafkatohive

import java.time.Duration
import java.util.Properties

import org.apache.log4j.Logger

class Parameters(args: Array[String])(implicit logger: Logger) {

  val paramMap: Map[String, String] = args.flatMap(argLine => argLine.split("==", 2) match {
    case Array(key, value) if value.nonEmpty => Some(key -> value)
    case _ =>
      logger.warn(s"Cannot parse parameter string or empty value: $argLine")
      None
  }).toMap

  logger.info("=====" * 4)
  paramMap.foreach {
    case (k, v) => logger.info(s"$k = $v")
  }
  logger.info("=====" * 4)

  def producerConfig: Properties = {
    val props: Properties = new Properties()
    props.put("bootstrap.servers", BOOTSTRAP_SERVERS)
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("acks", "all")
    props
  }

  def consumerConfig : Properties = {
    val props: Properties = new Properties()
    props.put("bootstrap.servers", BOOTSTRAP_SERVERS)
    props.put("group.id", GROUP_ID)
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("auto.offset.reset", "earliest")
    props
  }

  /** Common kafka parameters */
  val KAFKA_BIN_PATH: String = paramMap.getOrElse("KAFKA_BIN_PATH", """c:\\kafka\bin\windows\""")
  val SCRIPT_ENDING: String = paramMap.getOrElse("SCRIPT_ENDING", "")
  val BOOTSTRAP_SERVERS: String = paramMap.getOrElse("BOOTSTRAP_SERVERS", "localhost:9092")
  val MAIN_TOPIC: String = paramMap.getOrElse("MAIN_TOPIC", "pt_json")
  val LOG_TOPIC: String = paramMap.getOrElse("LOG_TOPIC", "pt_json_control")

  /** Kafka producer parameters */
  val RECORDS_NUMBER: Int = paramMap.getOrElse("RECORDS_NUMBER", "10").toInt
  val WRONG_VALUES_PERCENT: Int = paramMap.getOrElse("WRONG_VALUES_PERCENT", "100").toInt

  /** Kafka consumer parameters */
  val GROUP_ID: String = paramMap.getOrElse("GROUP_ID", "consumer-group")
  val TOPICS_TO_LOAD: String = paramMap.getOrElse("TOPICS_TO_LOAD", "all")
  val RESET_OFFSET_FLAG: Boolean = paramMap.getOrElse("RESET_OFFSET_FLAG", "false").toBoolean
  val POLL_DURATION: Duration = Duration.ofMillis(paramMap.getOrElse("POLL_DURATION", Long.MaxValue.toString).toLong)

  /** Hive common parameters */
  val SCHEMA: String = paramMap.getOrElse("SCHEMA", "")
  val CLIENT_TABLE: String = paramMap.getOrElse("CLIENT_TABLE", "")
  val CLIENT_PHONE_TABLE: String = paramMap.getOrElse("CLIENT_PHONE_TABLE", "")
  val LOG_TABLE: String = paramMap.getOrElse("LOG_TABLE", "")
  val LOAD_TYPE: String = paramMap.getOrElse("LOAD_TYPE", "test")
  val FORMAT_STORAGE : String = paramMap.getOrElse("FORMAT_STORAGE", "parquet")
  val DISABLE_CHECKS: Boolean = paramMap.getOrElse("DISABLE_CHECKS", "false").toBoolean

}
