package de.hpi.temporal_ind.data.column.data

import java.time.Instant

case class ColumnHistoryMetadata(id:String,uniqueness:collection.Map[Instant,Boolean], nChangeVersions:Int) {
  def toSerialized = ColumnHistoryMetadataJson(id,uniqueness.map(s => (s._1.toString,s._2)),nChangeVersions)

}

object ColumnHistoryMetadata {
  def readFromJsonObjectPerLineFile(outputFile: String) = ColumnHistoryMetadataJson
    .iterableFromJsonObjectPerLineFile(outputFile)
    .map(chm => chm.toColumnHistoryMetadata)
    .toIndexedSeq
    .map(chm => (chm.id,chm))
    .toMap
}
