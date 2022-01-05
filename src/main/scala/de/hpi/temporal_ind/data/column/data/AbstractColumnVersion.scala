package de.hpi.temporal_ind.data.column.data

import de.hpi.temporal_ind.util.Util

abstract class AbstractColumnVersion[T] {

  if(columnNotPresent)
    assert(values.isEmpty)

  def timestamp = Util.instantFromWikipediaDateTimeString(revisionDate)

  def isDelete = columnNotPresent

  def revisionID: String
  def revisionDate: String
  def values: Set[T]
  def columnNotPresent:Boolean
}

object AbstractColumnVersion {
  def INITIALEMPTYID: String = "-1"
}
