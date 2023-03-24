package de.hpi.temporal_ind.discovery

import de.hpi.temporal_ind.data.ind.INDCandidateIDs

object DiscrepancyExplorationMain extends App {
  val path1 = "/home/leon/data/temporalINDDiscovery/wikipedia/discovery/fromIsfet/0.8/discoveredINDs.jsonl"
  val path2 = "/home/leon/data/temporalINDDiscovery/wikipedia/discovery/fromIsfet/0.5/discoveredINDs.jsonl"
  val candidates1 = INDCandidateIDs.fromJsonObjectPerLineFile(path1)
    .groupBy(_.lhsColumnID)
  val candidates2 = INDCandidateIDs.fromJsonObjectPerLineFile(path2)
    .groupBy(_.lhsColumnID)
  println("intersection size",candidates1.keySet.intersect(candidates2.keySet).size)
  candidates1.keySet.intersect(candidates2.keySet).foreach(lhs => {
    val first = candidates1(lhs)
    val other = candidates2(lhs)
    if(first.toSet!=other.toSet){
      println()
    }
  })

}
