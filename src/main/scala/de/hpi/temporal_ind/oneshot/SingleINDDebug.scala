package de.hpi.temporal_ind.oneshot

import de.hpi.temporal_ind.data.column.data.IndexedColumnHistories
import de.hpi.temporal_ind.data.column.data.original.{INDCandidate, LabelledINDCandidateStatistics}
import de.hpi.temporal_ind.data.wikipedia.GLOBAL_CONFIG

import java.io.{File, PrintWriter}

object SingleINDDebug extends App {
  GLOBAL_CONFIG.setSettingsForDataSource("wikipedia")
  val dir = new File("/home/leon/data/temporalINDDiscovery/wikipedia/filteredHistories/")
  val pageIDLHS = 51881921L
  val pageIDRHS = 218747L
  val index = IndexedColumnHistories.loadForPageIDS(dir,IndexedSeq(pageIDLHS,pageIDRHS))
  val lhsColumnID = "597a29dd-020f-47d7-839b-6ff5e266dead"
  val rhsColumnID = "6e208986-af66-41ca-a73c-4a6d1f1d85cd"
  val candidate = INDCandidate.fromIDs(index,pageIDLHS,lhsColumnID,pageIDRHS,rhsColumnID)
  val pr = new PrintWriter("src/main/resources/tmp/tmpLabel.txt")
  val labelled = candidate.toLabelledINDCandidateStatistics("label")
  println(labelled)
  labelled.serializeValidityStatistics(pr)
  pr.close()

}
