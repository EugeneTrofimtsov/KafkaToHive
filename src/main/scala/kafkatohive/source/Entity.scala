package kafkatohive.source

import java.nio.charset.StandardCharsets

import org.apache.commons.codec.digest.DigestUtils

object Entity {

  case class Control(
                      dt: BigInt,
                      cnt: BigInt
                    )

  case class Information(
                          dt: BigInt,
                          msg: Message,
                          hashmsg: String
                        )

  case class Message(
                      firstname: String,
                      lastname: String,
                      patronymic: Option[String],
                      birthday: Option[String],
                      phone: Option[Array[String]],
                      address: Address
                    ) {
    def getHash(dt: BigInt): String = {
      val sb = new StringBuilder()
        .append(dt.toString)
        .append(firstname)
        .append(lastname)
        .append(patronymic.getOrElse(""))
        .append(birthday.getOrElse(""))
        .append(phone.getOrElse(Array("")).mkString)
        .append(address.fullAddress)
      DigestUtils.md5Hex(sb.toString.getBytes(StandardCharsets.UTF_8))
    }

    def getWrongHash(dt: BigInt): String = {
      val sb = new StringBuilder()
        .append(dt.toString.substring(1))
        .append(firstname.substring(1))
        .append(lastname.substring(1))
        .append(patronymic.getOrElse(""))
        .append(birthday.getOrElse(""))
        .append(phone.getOrElse(Array("")).mkString)
        .append(address.fullAddress.substring(1))
      DigestUtils.md5Hex(sb.toString.getBytes(StandardCharsets.UTF_8))
    }
  }

  case class Address(
                      zipcode: Option[String],
                      city: String,
                      code: String,
                      street: String,
                      house: String,
                      apartment: Option[String],
                      status: String
                    ) {
    def fullAddress: String = {
      new StringBuilder()
        .append(zipcode.getOrElse(""))
        .append(city)
        .append(code)
        .append(street)
        .append(house)
        .append(apartment.getOrElse(""))
        .append(status).toString
    }
  }

}
