package de.hpi.temporal_ind.discovery.statistics_and_results


case class TimeSliceStats(var numHistoriesWithVersionPresent: Int=0, var numVersionsPresentSum: Int=0, var hashedDistinctValues: collection.mutable.HashSet[Int]=collection.mutable.HashSet()) {


  def +=(other: TimeSliceStats) = {
    numHistoriesWithVersionPresent += other.numHistoriesWithVersionPresent
    numVersionsPresentSum += other.numVersionsPresentSum
    hashedDistinctValues ++= other.hashedDistinctValues
  }
}
