package de.hpi.temporal_ind.data.column.data.original

import de.hpi.temporal_ind.data.column.data.AbstractColumnVersion

import java.time.Instant

case class ChangePoint[T](timestamp:Instant,fkVersion:AbstractColumnVersion[T],pkVersion: AbstractColumnVersion[T],timeToNextChangeOrEndInMillis:Long) {
  //TODO!
}
