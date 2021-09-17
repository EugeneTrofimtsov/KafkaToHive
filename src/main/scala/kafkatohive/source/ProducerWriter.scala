package kafkatohive.source

import java.io._
import java.text.DecimalFormat
import java.util.concurrent.Future
import java.time.{Instant, LocalDate}
import java.time.format.DateTimeFormatter

import org.apache.log4j.Logger
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, RecordMetadata}
import net.liftweb.json._
import kafkatohive.Parameters
import kafkatohive.source.Data._
import kafkatohive.source.Entity._

import scala.collection.mutable.ListBuffer

object ProducerWriter {

  private val rand = new scala.util.Random(System.nanoTime)

  def getRandomDate(from: LocalDate, to: LocalDate): String = {
    LocalDate.ofEpochDay(from.toEpochDay + rand.nextInt((to.toEpochDay - from.toEpochDay).toInt))
      .format(DateTimeFormatter.ofPattern("dd.MM.yyyy"))
  }

  def getRandomPhone: String = {
    val num1 = (rand.nextInt(7) + 1) * 100 + (rand.nextInt(8) * 10) + rand.nextInt(8)
    val num2 = rand.nextInt(743)
    val num3 = rand.nextInt(10000)
    val format3 = new DecimalFormat("000")
    val format4 = new DecimalFormat("0000")
    format3.format(num1) + "-" + format3.format(num2) + "-" + format4.format(num3)
  }

  def getOptional[T](result: T): Option[T] = if (rand.nextInt(2) == 0) Some(result) else None

  def getRandomValue(dt: Long)(implicit conf: Parameters): Information = {
    val city = rand.nextInt(cities.length - 1)
    val msg = Message(
      firstnames(rand.nextInt(firstnames.length - 1)),
      lastnames(rand.nextInt(lastnames.length - 1)),
      patronymics.lift(rand.nextInt(patronymics.length - 1)),
      getOptional(getRandomDate(
        LocalDate.of(1950, 1, 1), LocalDate.of(2000, 1, 1)
      )),
      getOptional((for (_ <- 1 to 1 + rand.nextInt(3)) yield getRandomPhone).toArray),
      Address(
        getOptional((1000000 + rand.nextInt((9000000 - 1000000) + 1)).toString),
        cities(city),
        codes(city),
        streets(rand.nextInt(streets.length - 1)),
        rand.nextInt(100).toString,
        getOptional(rand.nextInt(1000).toString),
        statuses(rand.nextInt(statuses.length - 1))
      )
    )
    Information(
      dt,
      msg,
      if (rand.nextInt(100) + 1 > conf.WRONG_VALUES_PERCENT) msg.getHash(dt) else msg.getWrongHash(dt)
    )
  }

  def loadToFile(info: String)(implicit formats: DefaultFormats.type): Unit = {
    val fw = new FileWriter("test.json")
    fw.write(info)
    fw.close()
  }

  def loadToKafka(info: Seq[String], topic: String)
                 (implicit formats: DefaultFormats.type,
                  conf: Parameters, logger: Logger): ListBuffer[Future[RecordMetadata]] = {
    logger.info(s"Started writing to kafka topic '$topic'...")
    val producer = new KafkaProducer[String, String](conf.producerConfig)
    val responses = new ListBuffer[Future[RecordMetadata]]()
    try {
      for (elem <- info) {
        val record = new ProducerRecord[String, String](topic, elem)
        val metadata = producer.send(record)
        responses += metadata
        logger.info(s"Sent record(key=${record.key()} value=${record.value()}) " +
          s"meta(partition=${metadata.get().partition()}, offset=${metadata.get().offset()})")
      }
    } catch {
      case e: Exception =>
        logger.error(s"Problems while writing to kafka topic '$topic'")
        logger.error(e.getMessage)
    } finally {
      producer.close()
    }
    logger.info(s"Kafka writing to topic '$topic' complete...")
    logger.info(s"Number of wrote records: ${responses.length}")
    responses
  }

  def main(args: Array[String]): Unit = {
    implicit val logger: Logger = Logger.getLogger(this.getClass.getName)
    implicit val conf: Parameters = new Parameters(args)
    implicit val formats: DefaultFormats.type = DefaultFormats
    val dt = Instant.now.getEpochSecond
    val batch = for (_ <- 1 to conf.RECORDS_NUMBER) yield getRandomValue(dt)
    val responses = loadToKafka(batch.map(elem => compactRender(Extraction.decompose(elem))), conf.MAIN_TOPIC)
    loadToKafka(Seq(compactRender(Extraction.decompose(Control(dt, responses.length)))), conf.LOG_TOPIC)
    loadToFile(prettyRender(Extraction.decompose(batch)))
  }

}
