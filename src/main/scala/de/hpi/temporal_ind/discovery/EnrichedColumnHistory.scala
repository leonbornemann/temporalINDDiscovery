package de.hpi.temporal_ind.discovery

import de.hpi.temporal_ind.data.column.data.original.OrderedColumnHistory

class EnrichedColumnHistory(val och: OrderedColumnHistory,absoluteEpsilon:Long) {
  def colID: String = och.id
  def tableID: String = och.tableId


  def requiredValues = och.history.requiredValues(absoluteEpsilon).keys.toSet
  def allValues = och.allValues

}
