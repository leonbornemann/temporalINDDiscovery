package de.hpi.temporal_ind.data.attribute_history.data.original

import de.hpi.temporal_ind.data.GLOBAL_CONFIG
import de.hpi.temporal_ind.data.attribute_history.data.traversal.PeekableIterator
import de.hpi.temporal_ind.data.attribute_history.data.{AbstractColumnVersion, AbstractOrderdColumnVersionList}
import de.hpi.temporal_ind.data.ind.variant4.TimeUtil
import de.hpi.temporal_ind.discovery.TINDParameters

import java.time.Instant
import scala.collection.mutable

class OrderedColumnVersionList(val versions : mutable.TreeMap[Instant, _ <: AbstractColumnVersion[String]]) extends AbstractOrderdColumnVersionList[String] with Serializable{

  def versionsSorted= versions.toIndexedSeq.sortBy(_._1).map(_._2)

  //returns all values that exist for at least absoluteEpsilonNanos time
  def requiredValues(queryParameters: TINDParameters) = {
    val valueToWeight = collection.mutable.HashMap[String,Double]()
    val peekableVersionIterator = new PeekableIterator(versions.keysIterator)
    peekableVersionIterator.foreach{case v => {
      val nextTimestamp = peekableVersionIterator.peek.getOrElse(GLOBAL_CONFIG.lastInstant)
      val weight = queryParameters.omega.weight(v,nextTimestamp)
      versions(v).values
        .foreach(v => valueToWeight(v) = valueToWeight.getOrElseUpdate(v,0) + weight)
    }}
    valueToWeight
      .filter(t => t._2>queryParameters.absoluteEpsilon)
  }

}
