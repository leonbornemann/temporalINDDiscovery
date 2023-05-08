package de.hpi.temporal_ind.data.ind.variant4

import de.hpi.temporal_ind.data.attribute_history.data.AbstractColumnVersion

import java.time.Instant

case class VersionRange[T](beginInclusive: AbstractColumnVersion[T], endInclusive: AbstractColumnVersion[T]) {
  def borders = Set(beginInclusive,endInclusive)

}
