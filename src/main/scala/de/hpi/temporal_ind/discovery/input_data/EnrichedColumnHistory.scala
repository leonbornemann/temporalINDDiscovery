package de.hpi.temporal_ind.discovery.input_data

import de.hpi.temporal_ind.data.column.data.original.OrderedColumnHistory
import de.hpi.temporal_ind.discovery.TINDParameters

import java.time.Instant

class EnrichedColumnHistory(val och: OrderedColumnHistory) {
  def valueSetInWindow(beginDelta: Instant, endDelta: Instant): collection.Set[String] = och.valueSetInWindow(beginDelta,endDelta)

  //  def clearTimeWindowCache() = windowToValueSetMap.clear()
//
//  def valueSetInWindow(beginDelta: Instant, endDelta: Instant): collection.Set[String] = {
//    windowToValueSetMap.getOrElseUpdate((beginDelta,endDelta),och.valueSetInWindow(beginDelta,endDelta))
//  }
//
//  var windowToValueSetMap = collection.mutable.HashMap[(Instant,Instant),collection.Set[String]]()
  def colID: String = och.id
  def tableID: String = och.tableId


  def requiredValues(queryParameters:TINDParameters) = {
    och.history.requiredValues(queryParameters).keys.toSet
  }
  def allValues = och.allValues

}
