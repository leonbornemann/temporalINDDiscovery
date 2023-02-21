package de.hpi.temporal_ind.discovery

import de.hpi.temporal_ind.data.{JsonReadable, JsonWritable}
import de.hpi.temporal_ind.data.wikipedia.GLOBAL_CONFIG

import java.time.Instant

case class DiscoveryStatRow(queryLink: String,
                            queryPageID:String,
                            queryTableID:String,
                            queryColumnID:String,
                            indexQueryTimeMS: Double,
                            validationTimeMS: Double,
                            falsePositiveCount: Int,
                            truePositiveCount: Int,
                            discoveryVersion:String) extends JsonWritable[DiscoveryStatRow]{

  def toCSVLine = {
    s"$queryLink,$queryPageID,$queryTableID,$queryColumnID,$indexQueryTimeMS,$validationTimeMS,$falsePositiveCount,$truePositiveCount,$discoveryVersion"
  }

}
object DiscoveryStatRow extends JsonReadable[DiscoveryStatRow]{
  def schema = "queryLink,queryPageID,queryTableID,queryColumnID,indexQueryTime,validationTime,falsePositiveCount,truePositiveCount,discoveryVersion"

  def fromEnrichedColumnHistory(query: EnrichedColumnHistory,
                                indexQueryTimeMS: Double,
                                validationTimeMS: Double,
                                falsePositiveCount: Int,
                                truePositiveCount: Int,
                                discoveryVersion: String) = {
    val queryLink = query.och.activeRevisionURLAtTimestamp(GLOBAL_CONFIG.lastInstant)
    val queryPageID = query.och.pageID
    val queryTableID = query.och.tableId
    val queryColumnID = query.och.id
    DiscoveryStatRow(queryLink,
      queryPageID,
      queryTableID,
      queryColumnID,
      indexQueryTimeMS,
      validationTimeMS,
      falsePositiveCount,
      truePositiveCount,
      discoveryVersion)
  }
}


