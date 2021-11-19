package de.hpi.temporal_ind.data.column

import de.hpi.temporal_ind.data.wikipedia.GLOBAL_CONFIG
import de.hpi.temporal_ind.util.Util

import java.time.Instant

case class ColumnVersion(revisionID: String,
                         revisionDate: String,
                         values: Set[String]){
  def timestamp = Util.instantFromWikipediaDateTimeString(revisionDate)

  def isEmpty = values.isEmpty


}

object ColumnVersion {
  def INITIALEMPTYID: String = "-1"

  def EMPTY(revisionID: String, revisionDate: String): ColumnVersion = ColumnVersion(revisionID,revisionDate,Set())
}
