package de.hpi.temporal_ind.data.column.labelling

import de.hpi.temporal_ind.data.column.data.{IncrementalIndexedColumnHistories, IndexedColumnHistories}
import de.hpi.temporal_ind.data.column.data.original.LabelledINDCandidateStatistics
import de.hpi.temporal_ind.data.column.labelling.TINDCandidateExportForLabelling.columnHistoryDir
import de.hpi.temporal_ind.data.wikipedia.GLOBAL_CONFIG

import java.io.{File, PrintWriter}
import scala.io.Source

object StatisticsForLabelledExampleMain extends App {
  GLOBAL_CONFIG.setSettingsForDataSource(args(0))
  val sourceDir = new File(args(1))
  val index = new IncrementalIndexedColumnHistories(new File(args(2)))
  private val resultDir: String = args(3)
  new File(resultDir).mkdirs()

  sourceDir.listFiles().foreach(sourceFile => {
    val out = new PrintWriter(resultDir + "/stats_" + sourceFile.getName)
    LabelledINDCandidateStatistics.printCSVSchema(out)
    val examples = Source.fromFile(sourceFile)
      .getLines()
      .toIndexedSeq
      .tail
      .map(l => LabelledINDCandidateStatistics.fromCSVLineWithIncrementalIndex(index, l))
      .foreach(ind => ind.serializeValidityStatistics(out))
    out.close()
  })


}
