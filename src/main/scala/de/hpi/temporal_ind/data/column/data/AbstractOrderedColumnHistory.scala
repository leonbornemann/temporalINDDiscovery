package de.hpi.temporal_ind.data.column.data

import de.hpi.temporal_ind.data.wikipedia.GLOBAL_CONFIG

import java.time.Instant

abstract class AbstractOrderedColumnHistory[T] {

  def isLastVersionBefore(version:Instant,upperExclusive: Instant): Boolean = {
    val versionsInRange = history.versions.range(version,upperExclusive)
    !versionsInRange.isEmpty && versionsInRange.last._1==version
  }


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

  def versionsInWindow(lowerInclusive: Instant, upperExclusive: Instant) = {
    val res = history
      .versions
      .rangeImpl(Some(lowerInclusive),Some(upperExclusive))
      .keySet
    if (!history.versions.contains(lowerInclusive)) {
      //add previous version because it lasted until lowerInclusive
      res ++ history.versions.maxBefore(lowerInclusive).map(_._1).toSet
    } else {
      res
    }
  }

  def valuesInWindow(lowerInclusive: Instant, upperExclusive: Instant,limitToVersions:Option[collection.Set[Instant]]=None) :collection.Set[T] = {
    val res = collection.mutable.HashSet[T]()
    history.versions
      .rangeImpl(Some(lowerInclusive), Some(upperExclusive))
      .withFilter(t => limitToVersions.isEmpty || limitToVersions.get.contains(t._1))
      .foreach(t => res.addAll(t._2.values))
    if (!history.versions.contains(lowerInclusive)) {
      //add previous version because it lasted until lowerInclusive
      val toAdd = history.versions.maxBefore(lowerInclusive)
      if(toAdd.isDefined && (limitToVersions.isEmpty || limitToVersions.get.contains(toAdd.get._1)))
        toAdd.get._2.values ++ res
      else
        res
    } else {
      res
    }
  }

  def versionAt(v: Instant):AbstractColumnVersion[T]
}
