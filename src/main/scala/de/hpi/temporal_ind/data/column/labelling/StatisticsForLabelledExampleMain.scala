package de.hpi.temporal_ind.data.column.labelling

import de.hpi.temporal_ind.data.column.data.IndexedColumnHistories
import de.hpi.temporal_ind.data.column.data.original.LabelledINDCandidateStatistics
import de.hpi.temporal_ind.data.wikipedia.GLOBAL_CONFIG

import java.io.PrintWriter
import scala.io.Source

object StatisticsForLabelledExampleMain extends App {
  GLOBAL_CONFIG.setSettingsForDataSource(args(0))
  val sourceFile = args(1)
  val histories = IndexedColumnHistories.fromColumnHistoryJsonPerLineDir(args(2))
  val out = new PrintWriter(args(3))

  LabelledINDCandidateStatistics.printCSVSchema(out)
  val examples = Source.fromFile(sourceFile)
    .getLines()
    .toIndexedSeq
    .tail
    .map(l => LabelledINDCandidateStatistics.fromCSVLine(histories,l))
    .foreach(ind => ind.serializeValidityStatistics(out))
  out.close()

}
