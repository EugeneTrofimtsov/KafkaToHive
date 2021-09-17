package kafkatohive.target

import org.apache.spark.sql.types._

object Entity {

  val controlSchema = List(
    StructField("dt", TimestampType),
    StructField("dt_load", TimestampType),
    StructField("cnt", LongType)
  )

  val clientSchema = List(
    StructField("dt", TimestampType),
    StructField("dt_load", TimestampType),
    StructField("fio", StringType),
    StructField("birthday", StringType),
    StructField("address", StringType),
    StructField("phonecnt", ByteType),
    StructField("hashsrc", StringType),
    StructField("hashctrl", StringType),
    StructField("verify", BooleanType)
  )

  val clientPhoneSchema = List(
    StructField("dt", TimestampType),
    StructField("dt_load", TimestampType),
    StructField("fio", StringType),
    StructField("phone", StringType)
  )

}
