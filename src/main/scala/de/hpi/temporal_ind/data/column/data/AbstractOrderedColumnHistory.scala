package de.hpi.temporal_ind.data.column.data

import de.hpi.temporal_ind.data.wikipedia.GLOBAL_CONFIG

import java.time.Instant

abstract class AbstractOrderedColumnHistory[T] {

  def activeRevisionURLAtTimestamp(version: Instant) = {
    //https://en.wikipedia.org/?curid=368629&oldid=1084266355
    val revisionID = if(history.versions.contains(version)){
      history.versions(version).revisionID
    } else {
      history
        .versions
        .maxBefore(version)
        .get._2
        .revisionID
    }
    s"https://en.wikipedia.org/?curid=$pageID&oldid=$revisionID"
  }


  /***
   *
   * @return an ordered sequence of disjoint intervals [t,t') during which data was present
   */
  def nonEmptyIntervals = {
    val withIndex = history.versions.toIndexedSeq.zipWithIndex
    val intervals = withIndex.filter(!_._1._2.isDelete).map{case ((t,_),i) =>
      val endPoint = if(i == withIndex.size-1) GLOBAL_CONFIG.lastInstant else withIndex(i+1)._1._1
      (t,endPoint)
    }
    new TimeIntervalSequence(intervals)
  }


  def id: String
  def tableId: String
  def pageID: String
  def pageTitle: String
  def history: AbstractOrderdColumnVersionList[T]

  def versionsInWindow(lowerInclusive: Instant, upperInclusive: Instant) = history
    .versions
    .rangeImpl(Some(lowerInclusive),Some(upperInclusive.plusNanos(1)))
    .keySet

  def valuesInWindow(lowerInclusive: Instant, upperInclusive: Instant) :Set[T] = {
    val res = history.versions
      .rangeImpl(Some(lowerInclusive), Some(upperInclusive.plusNanos(1)))
      .flatMap(_._2.values)
      .toSet
    if (!history.versions.contains(lowerInclusive)) {
      //add previous version because it lasted until lowerInclusive
      val toAdd = history.versions.maxBefore(lowerInclusive)
      toAdd.map(_._2.values).getOrElse(Set()) ++ res
    } else {
      res
    }
  }

  def versionAt(v: Instant):AbstractColumnVersion[T]
}
