package de.hpi.temporal_ind.data.column.data.original

import de.hpi.temporal_ind.data.column.data.{AbstractOrderedColumnHistory, IndexedColumnHistories}
import de.hpi.temporal_ind.data.{JsonReadable, JsonWritable}
import de.hpi.temporal_ind.util.Util

import java.time.Instant

case class INDCandidate[T](lhs: AbstractOrderedColumnHistory[T], rhs:AbstractOrderedColumnHistory[T]) {

  def toLabelCSVString(version:Instant):String = {
    val pkVersion = rhs
      .versionAt(version)
    val firstPkValuesAsString = s"(${pkVersion.values.size}) " + pkVersion
      .values
      .take(10)
      .mkString(";")
      .appended('}')
      .prepended('{')
    val fkVersion = lhs
      .versionAt(version)
  val firstFKValuesAsString = s"(${fkVersion.values.size}) "+ fkVersion
      .values
      .take(10)
      .mkString(";")
      .appended('}')
      .prepended('{')
    val fkID = lhs.id
    val pkID = rhs.id
    val fkPageID = lhs.pageID
    val pkPageID = rhs.pageID
    val fkTitle = lhs.pageTitle
    val pkTitle = rhs.pageTitle
    val versionURLFK = lhs.activeRevisionURLAtTimestamp(version)
    val versionURLPK = rhs.activeRevisionURLAtTimestamp(version)
    val pkTableID = rhs.tableId
    val fkTableID = lhs.tableId
    //what do we want additionally(?)
    //remainsValidPercentage
    //remainsValidPercentageWildcardLogic
    //isValidForWhichDelta/Epsilon
    Seq(versionURLFK,versionURLPK,"null",fkTableID,pkTableID,firstFKValuesAsString,firstPkValuesAsString,fkID,pkID,fkPageID,pkPageID,fkTitle,pkTitle) //substitute null for label for now
      .map(s => Util.makeStringCSVSafe(s))
      .mkString(",")
  }

  def remainsValidPercentage = new CommonPointOfInterestIterator(lhs,rhs)

}

object INDCandidate {
  def fromCSVLine(index: IndexedColumnHistories, l: String) = {
    val tokens = csvSchema.split(",")
        .zip(l.split(",").toSeq)
        .toMap
    val lhs = index.multiLevelIndex(tokens("fkPageID"))(tokens("fkID"))
    val rhs = index.multiLevelIndex(tokens("pkPageID"))(tokens("pkID"))
    INDCandidate(lhs.asOrderedHistory,rhs.asOrderedHistory)
  }

  def csvSchema = "versionURLFK,versionURLPK,isGenuine,fkTableID,pkTableID,fkValues,pkValues,fkID,pkID,fkPageID,pkPageID,fkTitle,pkTitle"

}
