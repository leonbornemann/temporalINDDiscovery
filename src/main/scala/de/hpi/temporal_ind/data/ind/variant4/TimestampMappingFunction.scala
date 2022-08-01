package de.hpi.temporal_ind.data.ind.variant4

import de.hpi.temporal_ind.data.column.data.TimeIntervalSequence
import de.hpi.temporal_ind.data.wikipedia.GLOBAL_CONFIG

import java.time.Instant

case class TimestampMappingFunction(mappingFunction: Map[(Instant,Instant), (Instant,Instant)]) {

  //we want to fully cover the entire time period!
  private val coveredLHS = new TimeIntervalSequence(mappingFunction.keySet.toIndexedSeq.sortBy(_._1),true).intervals
  assert(coveredLHS.size==1)
  assert(coveredLHS(0)._1==GLOBAL_CONFIG.earliestInstant)
  assert(coveredLHS(0)._2==GLOBAL_CONFIG.lastInstant)
}
