package de.hpi.temporal_ind.util

import java.time.{LocalDateTime, ZoneId}
import java.time.format.DateTimeFormatter

object Util {
  def numberRegex = "-?[0-9]+[\\.,][0-9]+"//"TODO: Borrow from natural key discovery"

  val wikipediaDateTimeFormatter = DateTimeFormatter.ofPattern("E MMM dd HH:mm:ss z yyyy")

  def instantFromWikipediaDateTimeString(str:String) = {
    val dateTime = LocalDateTime.parse(str,wikipediaDateTimeFormatter)
    dateTime.atZone(ZoneId.of("UTC")).toInstant()
  }
}
