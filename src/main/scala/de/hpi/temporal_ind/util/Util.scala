package de.hpi.temporal_ind.util

import java.time.{LocalDateTime, ZoneId}
import java.time.format.DateTimeFormatter
import scala.collection.mutable.ArrayBuffer

object Util {

  def Jaccard_Similarity(set1: Set[String], set2: Set[String]) = {
    set1.intersect(set2).size / set1.union(set2).size.toDouble
  }

  def makeStringCSVSafe(s: String) = {
    s.replace(',',';')
      .replace('\r',' ')
      .replace('\n',' ')
      .replace('"','\'')
      .replace('\\','/')
  }

  def numberRegex = "-?[0-9]+[\\.,][0-9]+"//"TODO: Borrow from natural key discovery"

  val wikipediaDateTimeFormatter = DateTimeFormatter.ofPattern("E MMM dd HH:mm:ss z yyyy")

  def instantFromWikipediaDateTimeString(str:String) = {
    val dateTime = LocalDateTime.parse(str,wikipediaDateTimeFormatter)
    dateTime.atZone(ZoneId.of("UTC")).toInstant()
  }
}
