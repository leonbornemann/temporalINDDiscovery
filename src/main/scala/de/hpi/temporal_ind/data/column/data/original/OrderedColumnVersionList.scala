package de.hpi.temporal_ind.data.column.data.original

import de.hpi.temporal_ind.data.column.data.{AbstractColumnVersion, AbstractOrderdColumnVersionList}
import de.hpi.temporal_ind.data.ind.variant4.TimeUtil
import de.hpi.temporal_ind.data.wikipedia.GLOBAL_CONFIG

import java.time.Instant
import scala.collection.mutable

class OrderedColumnVersionList(val versions : mutable.TreeMap[Instant, _ <: AbstractColumnVersion[String]]) extends AbstractOrderdColumnVersionList[String] with Serializable{

  def versionsSorted= versions.toIndexedSeq.sortBy(_._1).map(_._2)

  //returns all values that exist for at least absoluteEpsilonNanos time
  def requiredValues(absoluteEpsilonNanos: Long) = {
    val valueToLifetime = collection.mutable.HashMap[String,Long]()
    val peekableVersionIterator = new PeekableIterator(versions.keysIterator)
    peekableVersionIterator.foreach{case v => {
      val nextTimestamp = peekableVersionIterator.peek.getOrElse(GLOBAL_CONFIG.lastInstant)
      val duration = TimeUtil.durationNanos(v,nextTimestamp)
      versions(v).values
        .foreach(v => valueToLifetime(v) = valueToLifetime.getOrElseUpdate(v,0) + duration)
    }}
    valueToLifetime
      .filter(t => t._2>absoluteEpsilonNanos)
  }

}
