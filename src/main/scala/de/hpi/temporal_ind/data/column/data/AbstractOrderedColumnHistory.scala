package de.hpi.temporal_ind.data.column.data

import de.hpi.temporal_ind.data.column.data.original.ColumnVersion
import de.hpi.temporal_ind.data.wikipedia.GLOBAL_CONFIG

import java.time.Instant

abstract class AbstractOrderedColumnHistory[T] {

  /***
   *
   * @return an ordered sequence of disjoint intervals [t,t') during which data was present
   */
  def nonEmptyIntervals = {
    val withIndex = history.versions.toIndexedSeq.zipWithIndex
    val intervals = withIndex.filter(!_._1._2.isDelete).map{case ((t,_),i) =>
      val endPoint = if(i == withIndex.size-1) GLOBAL_CONFIG.latestInstantWikipedia else withIndex(i+1)._1._1
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
