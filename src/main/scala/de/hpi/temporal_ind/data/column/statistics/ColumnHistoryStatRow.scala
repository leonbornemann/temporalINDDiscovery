package de.hpi.temporal_ind.data.column.statistics

import de.hpi.temporal_ind.data.column.data.original.ColumnHistory
import de.hpi.temporal_ind.data.column.statistics
import de.hpi.temporal_ind.data.wikipedia.GLOBAL_CONFIG
import de.hpi.temporal_ind.util.Util

case class ColumnHistoryStatRow(ch: ColumnHistory) {

  val nVersions = ch.columnVersions.size
  val valueChangeVersions = ch.versionsWithNonDeleteChanges
  val durationInDays = ch.firstInsertToLastDeleteTimeInDays(GLOBAL_CONFIG.lastInstant)
  val nVersionsWithChanges = valueChangeVersions.size
  val lifetimeInDays = ch.totalLifeTimeInDays(GLOBAL_CONFIG.lastInstant)
  val valueSets = valueChangeVersions.map(_.values)
  val valueSetSizes = valueSets.map(_.size)
  val sizeStatistics = ValueSequenceStatistics(valueSetSizes.map(_.toDouble))

  def toCSVLine = {
    val values = Seq(
      ch.id,
      ch.tableId,
      ch.pageID,
      ch.pageTitle,
      nVersions,
      nVersionsWithChanges,
      durationInDays,
      lifetimeInDays,
      sizeStatistics.max,
      sizeStatistics.min,
      sizeStatistics.mean,
      sizeStatistics.median,
    ).map(v => Util.makeStringCSVSafe(v.toString))
    values.mkString(",")
  }

}
object ColumnHistoryStatRow {
  //id: String,
  //                         tableId: String,
  //                         pageID: String,
  //                         pageTitle: String,
  def getSchema = Seq(
    "ch.id",
    "ch.tableId",
    "ch.pageID",
    "ch.pageTitle",
    "nVersions",
    "nVersionsWithChanges",
    "durationInDays",
    "lifetimeInDays",
    "sizeStatistics.max",
    "sizeStatistics.min",
    "sizeStatistics.mean",
    "sizeStatistics.median"
  )
}
