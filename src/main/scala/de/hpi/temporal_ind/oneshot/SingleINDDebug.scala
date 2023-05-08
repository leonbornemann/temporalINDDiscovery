package de.hpi.temporal_ind.oneshot

import de.hpi.temporal_ind.data.GLOBAL_CONFIG
import de.hpi.temporal_ind.data.attribute_history.data.file_search.IndexedColumnHistories
import de.hpi.temporal_ind.data.attribute_history.data.many.InclusionDependencyFromMany
import de.hpi.temporal_ind.data.attribute_history.data.metadata.LabelledINDCandidateStatistics
import de.hpi.temporal_ind.data.attribute_history.labelling.TINDCandidateMetricDisagreementExport.deltas
import de.hpi.temporal_ind.data.ind.INDCandidate
import de.hpi.temporal_ind.data.ind.variant4.TimeUtil

import java.io.{File, PrintWriter}

object SingleINDDebug extends App {
  GLOBAL_CONFIG.setSettingsForDataSource("wikipedia")
  val dir = new File("/home/leon/data/temporalINDDiscovery/wikipedia/filteredHistories/")
  val candidate = InclusionDependencyFromMany.fromManyOutputString("[757045939-0_52710831.csv.32291dae-7dca-4ac1-8d29-d45d04c1d02b][=[755424309-1_52615934.csv.04d5a148-cf11-4841-9367-5428a4d5b965]")
  val index = IndexedColumnHistories.loadForPageIDS(dir,IndexedSeq(candidate.rhsPageID.toLong,candidate.lhsPageID.toLong))
  val pr = new PrintWriter("src/main/resources/tmp/tmpLabel.txt")
  val labelled = candidate
    .toCandidate(index,true)
    .toLabelledINDCandidateStatistics("label")
  labelled.serializeValidityStatistics(pr,None)
  pr.close()
}
