package de.hpi.temporal_ind.data.attribute_history.data

import java.time.Instant

case class ChangePoint[T](timestamp:Instant,fkVersion:AbstractColumnVersion[T],pkVersion: AbstractColumnVersion[T],timeToNextChangeOrEndInMillis:Long) {
  //TODO!
}
