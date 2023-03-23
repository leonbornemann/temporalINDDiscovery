package de.hpi.temporal_ind.discovery

import de.hpi.temporal_ind.data.ind.ShifteddRelaxedCustomFunctionTemporalIND

import scala.collection.mutable.ArrayBuffer

trait ResultSerializer {
  def addTrueTemporalINDs(trueTemporalINDs: ArrayBuffer[ShifteddRelaxedCustomFunctionTemporalIND[String]])


  def addIndividualResultStats(individualStatLine: IndividualResultStats)

  def addBasicQueryInfoRow(validationStatRow: BasicQueryInfoRow)

  def closeAll()

  def addTotalResultStats(totalResultSTatsLine: TotalResultStats)

}