package de.hpi.temporal_ind.data.column.data

import de.hpi.temporal_ind.data.column.data.original.ColumnVersion

import java.time.Instant
import scala.collection.mutable

abstract class AbstractOrderdColumnVersionList[T] {

  def versions : mutable.TreeMap[Instant, _ <: AbstractColumnVersion[T]]
}
