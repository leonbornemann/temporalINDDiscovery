package de.hpi.temporal_ind.data.column.labelling

import de.hpi.temporal_ind.data.column.data.{IncrementalIndexedColumnHistories, IndexedColumnHistories}
import de.hpi.temporal_ind.data.column.data.original.LabelledINDCandidateStatistics
import de.hpi.temporal_ind.data.column.labelling.TINDCandidateExportForLabelling.columnHistoryDir
import de.hpi.temporal_ind.data.wikipedia.GLOBAL_CONFIG

import java.io.{File, PrintWriter}
import scala.io.Source

object StatisticsForLabelledExampleMain extends App {
  GLOBAL_CONFIG.setSettingsForDataSource(args(0))
  val annotatedSource = new File(args(1))
  val index = new IncrementalIndexedColumnHistories(new File(args(2)))
  val prTimes = new PrintWriter("times.csv")
  prTimes.println("timeOld,timeNew")
  private val resultDir: String = args(3)
  new File(resultDir).mkdirs()


  if(annotatedSource.isDirectory){
    annotatedSource.listFiles().foreach(sourceFile => processFile(sourceFile))
  } else {
    processFile(annotatedSource)
  }
  prTimes.close()

  private def processFile(sourceFile: File): Unit = {
    println(s"Processing $sourceFile")
    val out = new PrintWriter(resultDir + "/stats_" + sourceFile.getName)
    val validLabels = Set("FALSE","TRUE","INTERESTING")
    LabelledINDCandidateStatistics.printCSVSchema(out)
    Source.fromFile(sourceFile)
      .getLines()
      .toIndexedSeq
      .tail
      .withFilter(l => validLabels.contains(l.split(",")(8)))
      .map(l => LabelledINDCandidateStatistics.fromCSVLineWithIncrementalIndex(index, l))
      .foreach(ind => ind.serializeValidityStatistics(out,Some(prTimes)))
    out.close()
  }
}
