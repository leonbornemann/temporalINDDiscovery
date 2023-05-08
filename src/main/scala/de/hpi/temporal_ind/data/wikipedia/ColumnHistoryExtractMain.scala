package de.hpi.temporal_ind.data.wikipedia

import de.hpi.temporal_ind.data.GLOBAL_CONFIG

import java.io.{File, PrintWriter}

object ColumnHistoryExtractMain extends App {
  GLOBAL_CONFIG.setSettingsForDataSource(args(0))
  val inputFile = args(1)
  val outputDir = args(2)
  val outputFile = new File(outputDir).getAbsolutePath + "/" + new File(inputFile).getName
  val pr = new PrintWriter(outputFile)
  val histories = TableHistory.iterableFromJsonObjectPerLineFile(inputFile)
  val preparer = new WikipediaDataPreparer()
  histories.foreach(th => {
    val columnHistories = preparer.extractColumnLineagesFromTableHistory(th)
    columnHistories
      .withFilter(ch => ch.columnVersions.exists(cv => !cv.values.isEmpty))
      .foreach(_.appendToWriter(pr))
  })
  pr.close()

}
