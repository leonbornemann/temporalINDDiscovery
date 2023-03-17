package de.hpi.temporal_ind.discovery

import de.hpi.temporal_ind.data.column.data.original.OrderedColumnHistory

import java.time.Instant

class EnrichedColumnHistory(val och: OrderedColumnHistory,absoluteEpsilon:Long) {
  def valueSetInWindow(beginDelta: Instant, endDelta: Instant): collection.Set[String] = {
    windowToValueSetMap.getOrElseUpdate((beginDelta,endDelta),och.valueSetInWindow(beginDelta,endDelta))
  }

  var windowToValueSetMap = collection.mutable.HashMap[(Instant,Instant),collection.Set[String]]()
  var requiredValuesVar:Option[Set[String]] = None
  def colID: String = och.id
  def tableID: String = och.tableId


  def requiredValues = {
    if(requiredValuesVar.isDefined) {
      requiredValuesVar.get
    }
    else{
      requiredValuesVar = Some(och.history.requiredValues(absoluteEpsilon).keys.toSet)
      requiredValuesVar.get
    }

  }
  def allValues = och.allValues

}
