package de.hpi.temporal_ind.discovery

import de.hpi.temporal_ind.data.{JsonReadable, JsonWritable}
import de.hpi.temporal_ind.data.wikipedia.GLOBAL_CONFIG

import java.time.Instant

case class DiscoveryStatRow(query: EnrichedColumnHistory,
                            indexQueryTimeMS: Double,
                            validationTimeMS: Double,
                            falsePositiveCount: Int,
                            truePositiveCount: Int,
                            discoveryVersion:String) extends JsonWritable[DiscoveryStatRow]{

  def queryLink = query.och.activeRevisionURLAtTimestamp(GLOBAL_CONFIG.lastInstant)
  def queryPageID = query.och.pageID
  def queryTableID = query.och.tableId
  def queryColumnID = query.och.id

  def toCSVLine = {
    s"$queryLink,$queryPageID,$queryTableID,$queryColumnID,$indexQueryTimeMS,$validationTimeMS,$falsePositiveCount,$truePositiveCount,$discoveryVersion"
  }

}
object DiscoveryStatRow extends JsonReadable[DiscoveryStatRow]{
  def schema = "queryLink,queryPageID,queryTableID,queryColumnID,indexQueryTime,validationTime,falsePositiveCount,truePositiveCount,discoveryVersion"
}


