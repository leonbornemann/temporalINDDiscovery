package de.hpi.temporal_ind.discovery

import de.hpi.temporal_ind.data.column.data.AbstractColumnVersion


case class TimeSliceStats(var numHistoriesWithVersionPresent: Int=0, var numVersionsPresentSum: Int=0, var hashedDistinctValues: collection.mutable.HashSet[Int]=collection.mutable.HashSet()) {


  def +=(other: TimeSliceStats) = {
    numHistoriesWithVersionPresent += other.numHistoriesWithVersionPresent
    numVersionsPresentSum += other.numVersionsPresentSum
    hashedDistinctValues ++= other.hashedDistinctValues
  }
}
