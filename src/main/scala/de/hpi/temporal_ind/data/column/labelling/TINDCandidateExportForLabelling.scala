package de.hpi.temporal_ind.data.column.labelling

import de.hpi.temporal_ind.data.column.data.IndexedColumnHistories
import de.hpi.temporal_ind.data.column.data.many.InclusionDependencyFromMany
import de.hpi.temporal_ind.data.column.data.original.{INDCandidate, LabelledINDCandidateStatistics}
import de.hpi.temporal_ind.data.wikipedia.GLOBAL_CONFIG

import java.io.{File, PrintWriter}

object TINDCandidateExportForLabelling extends App {
  println(s"called with ${args.toIndexedSeq}")
  GLOBAL_CONFIG.setSettingsForDataSource(args(0))
  val inputFile = args(1)
  val columnHistoryDir = args(2)
  val outputFileToLabel = args(3)
  val outputFileStatistics = args(4)
  val version = GLOBAL_CONFIG.lastInstant
  val sampleSize = 2000
  val pr = new PrintWriter(outputFileToLabel)
  pr.println(INDCandidate.csvSchema)
  val prStatistics = new PrintWriter(outputFileStatistics)
  LabelledINDCandidateStatistics.printCSVSchema(prStatistics)
  val indexed = IndexedColumnHistories.fromColumnHistoryJsonPerLineDir(columnHistoryDir)
  InclusionDependencyFromMany.readFromMANYOutputFile(new File(inputFile))
    .take(sampleSize)
    .map(ind => ind.toCandidate(indexed))
    .foreach(candidate => {
      pr.println(candidate.toLabelCSVString(version))
      val labelledCandidate = LabelledINDCandidateStatistics("null",candidate)
      labelledCandidate.serializeValidityStatistics(prStatistics)
    })
  pr.close()
  prStatistics.close()
}
