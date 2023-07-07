package de.hpi.temporal_ind.discovery.input_data

import de.hpi.temporal_ind.data.attribute_history.data.original.OrderedColumnHistory
import de.hpi.temporal_ind.discovery.TINDParameters

import java.time.Instant

class EnrichedColumnHistory(val och: OrderedColumnHistory) {
  def valueSetInWindow(beginDelta: Instant, endDelta: Instant): collection.Set[String] = och.valueSetInWindow(beginDelta,endDelta)

  def colID: String = och.id
  def tableID: String = och.tableId


  def requiredValues(queryParameters:TINDParameters) = {
    och.history.requiredValues(queryParameters).keys.toSet
  }
  def allValues = och.allValues

  def getValueSetsInWindow(begin: Instant, end: Instant) = {
    val withIndex = och.versionsInWindowNew(begin, end)
      .toIndexedSeq
      .zipWithIndex
    val queryValueSets = withIndex
      .map { case ((begin, version), i) =>
        val curEnd = if (i == withIndex.size - 1) end else withIndex(i + 1)._1._1
        new ValuesInTimeWindow(begin, curEnd, version.values)
      }
    queryValueSets
  }

}
