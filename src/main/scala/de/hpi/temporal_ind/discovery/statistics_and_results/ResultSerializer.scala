package de.hpi.temporal_ind.discovery.statistics_and_results

import de.hpi.temporal_ind.data.ind.EpsilonOmegaDeltaRelaxedTemporalIND

import scala.collection.mutable.ArrayBuffer

trait ResultSerializer {
  def addTrueTemporalINDs(trueTemporalINDs: Iterable[EpsilonOmegaDeltaRelaxedTemporalIND[String]])


  def addIndividualResultStats(individualStatLine: IndividualResultStats)

  def addBasicQueryInfoRow(validationStatRow: BasicQueryInfoRow)

  def closeAll()

  def addTotalResultStats(totalResultSTatsLine: TotalResultStats)


}