package de.hpi.temporal_ind.data.wikipedia

import java.io.{File, PrintWriter}

object ColumnHistoryExtractMain extends App {
  val inputFile = args(0)
  val outputDir = args(1)
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
