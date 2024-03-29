package de.hpi.temporal_ind.data.ind

import de.hpi.temporal_ind.data.attribute_history.data.AbstractOrderedColumnHistory
import de.hpi.temporal_ind.data.attribute_history.data.file_search.{IncrementalIndexedColumnHistories, IndexedColumnHistories}
import de.hpi.temporal_ind.data.attribute_history.data.metadata.LabelledINDCandidateStatistics
import de.hpi.temporal_ind.data.attribute_history.data.traversal.CommonPointOfInterestIterator
import de.hpi.temporal_ind.util.Util

import java.time.Instant

case class INDCandidate[T <% Ordered[T]](lhs: AbstractOrderedColumnHistory[T], rhs:AbstractOrderedColumnHistory[T]) {
  def toLabelledINDCandidateStatistics(label: String) = LabelledINDCandidateStatistics(label,this)


  def toLabelCSVString(versionLHS:Instant,versionRHSOption:Option[Instant]=None):String = {
    val versionRHS = if(versionRHSOption.isEmpty) versionLHS else versionRHSOption.get
    val pkVersion = rhs
      .versionAt(versionLHS)
    val firstPkValuesAsString = s"(${pkVersion.values.size}) " + pkVersion
      .values
      .take(10)
      .mkString(";")
      .appended('}')
      .prepended('{')
    val fkVersion = lhs
      .versionAt(versionRHS)
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
    val versionURLFK = lhs.activeRevisionURLAtTimestamp(versionLHS)
    val versionURLPK = rhs.activeRevisionURLAtTimestamp(versionRHS)
    val pkTableID = rhs.tableId
    val fkTableID = lhs.tableId
    //what do we want additionally(?)
    //remainsValidPercentage
    //remainsValidPercentageWildcardLogic
    //isValidForWhichDelta/Epsilon
    Seq(versionURLFK,
      versionURLPK,
      lhs.pageTitle,
      lhs.history.versions.last._2.header.getOrElse(""),
      lhs.history.versions.last._2.position.getOrElse(-1).toString,
      rhs.pageTitle,
      rhs.history.versions.last._2.header.getOrElse(""),
      rhs.history.versions.last._2.position.getOrElse(-1).toString,
      "null",
      fkTableID,
      pkTableID,
      firstFKValuesAsString,
      firstPkValuesAsString,
      fkID,
      pkID,
      fkPageID,
      pkPageID,
      fkTitle,
      pkTitle) //substitute null for label for now
      .map(s => Util.makeStringCSVSafe(s))
      .mkString(",")
    //pageTitleFK,colHeaderFK,colPositionFK,pageTitlePK,colHeaderPK,colPositionPK
  }

  def remainsValidPercentage = new CommonPointOfInterestIterator(lhs,rhs)

}

object INDCandidate {
  def fromIDs(index: IndexedColumnHistories, pageIDLHS: Long, lhsColumnID: String, pageIDRHS: Long, rhsColumnID: String) = {
    val lhs = index.multiLevelIndex(pageIDLHS.toString)(lhsColumnID)
    val rhs = index.multiLevelIndex(pageIDRHS.toString)(rhsColumnID)
    INDCandidate(lhs.asOrderedHistory,rhs.asOrderedHistory)
  }

  def fromCSVLine(index: IndexedColumnHistories, l: String) = {
    val tokens = csvSchema.split(",")
        .zip(l.split(",").toSeq)
        .toMap
    val lhs = index.multiLevelIndex(tokens("fkPageID"))(tokens("fkID"))
    val rhs = index.multiLevelIndex(tokens("pkPageID"))(tokens("pkID"))
    INDCandidate(lhs.asOrderedHistory,rhs.asOrderedHistory)
  }

  def fromCSVLineWithIncrementalIndex(index: IncrementalIndexedColumnHistories, l: String) = {
    val tokens = csvSchema.split(",")
      .zip(l.split(",").toSeq)
      .toMap
    val lhs = index.getOrLoad(tokens("fkPageID").toLong,tokens("fkID"))
    val rhs = index.getOrLoad(tokens("pkPageID").toLong,tokens("pkID"))
    INDCandidate(lhs.asOrderedHistory, rhs.asOrderedHistory)
  }

  def csvSchema = "versionURLFK,versionURLPK,pageTitleFK,colHeaderFK,colPositionFK,pageTitlePK,colHeaderPK,colPositionPK,isGenuine,fkTableID,pkTableID,fkValues,pkValues,fkID,pkID,fkPageID,pkPageID,fkTitle,pkTitle"

}
