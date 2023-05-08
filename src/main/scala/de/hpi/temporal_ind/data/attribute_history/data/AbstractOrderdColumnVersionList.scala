package de.hpi.temporal_ind.data.attribute_history.data

import de.hpi.temporal_ind.data.attribute_history.data.original.ColumnVersion

import java.time.Instant
import scala.collection.mutable

abstract class AbstractOrderdColumnVersionList[T] extends Serializable{

  def versions : mutable.TreeMap[Instant, _ <: AbstractColumnVersion[T]]
}
