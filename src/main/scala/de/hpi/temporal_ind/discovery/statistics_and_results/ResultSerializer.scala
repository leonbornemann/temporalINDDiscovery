package de.hpi.temporal_ind.discovery.statistics_and_results

import de.hpi.temporal_ind.data.ind.ShifteddRelaxedCustomFunctionTemporalIND

import scala.collection.mutable.ArrayBuffer

trait ResultSerializer {
  def addTrueTemporalINDs(trueTemporalINDs: Iterable[ShifteddRelaxedCustomFunctionTemporalIND[String]])


  def addIndividualResultStats(individualStatLine: IndividualResultStats)

  def addBasicQueryInfoRow(validationStatRow: BasicQueryInfoRow)

  def closeAll()

  def addTotalResultStats(totalResultSTatsLine: TotalResultStats)


}