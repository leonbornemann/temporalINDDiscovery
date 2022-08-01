package de.hpi.temporal_ind.data.column.data.original

import de.hpi.temporal_ind.data.column.data.AbstractOrderedColumnHistory
import de.hpi.temporal_ind.data.{JsonReadable, JsonWritable}
import de.hpi.temporal_ind.util.Util

import java.time.Instant

case class INDCandidate[T](fkCandidate: AbstractOrderedColumnHistory[T], pkCandidate:AbstractOrderedColumnHistory[T]) {

  def toLabelCSVString(version:Instant):String = {
    val firstPkValuesAsString = pkCandidate
      .versionAt(version)
      .values
      .take(10)
      .mkString(";")
      .appended('}')
      .prepended('{')
    val firstFKValuesAsString = fkCandidate
      .versionAt(version)
      .values
      .take(10)
      .mkString(";")
      .appended('}')
      .prepended('{')
    val fkID = fkCandidate.id
    val pkID = pkCandidate.id
    val fkPageID = fkCandidate.pageID
    val pkPageID = pkCandidate.pageID
    val fkTitle = fkCandidate.pageTitle
    val pkTitle = pkCandidate.pageTitle
    val versionURLFK = fkCandidate.activeRevisionURLAtTimestamp(version)
    val versionURLPK = pkCandidate.activeRevisionURLAtTimestamp(version)
    val pkTableID = pkCandidate.tableId
    val fkTableID = fkCandidate.tableId
    //what do we want additionally(?)
    //remainsValidPercentage
    //remainsValidPercentageWildcardLogic
    //isValidForWhichDelta/Epsilon
    Seq(versionURLFK,versionURLPK,"null",fkTableID,pkTableID,firstFKValuesAsString,firstPkValuesAsString,fkID,pkID,fkPageID,pkPageID,fkTitle,pkTitle) //substitute null for label for now
      .map(s => Util.makeStringCSVSafe(s))
      .mkString(",")
  }

  def remainsValidPercentage = new CommonPointOfInterestIterator(fkCandidate,pkCandidate)

}

object INDCandidate {
  def csvSchema = "versionURLFK,versionURLPK,isGenuine,fkTableID,pkTableID,fkValues,pkValues,fkID,pkID,fkPageID,pkPageID,fkTitle,pkTitle"

}
