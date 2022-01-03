package de.hpi.temporal_ind.data.column.statistics

import de.hpi.temporal_ind.data.column.{ColumnHistory, statistics}

import java.io.{File, PrintWriter}

object StatisticsExportMain extends App {
  val inputDir = new File(args(0))
  val resultFile = new PrintWriter(args(1))
  val iterable = ColumnHistory.iterableFromJsonObjectPerLineDir(inputDir, true)
  resultFile.println(ColumnHistoryStatRow.getSchema.mkString(","))
  iterable.foreach(ch => {
    val statRow = statistics.ColumnHistoryStatRow(ch)
    resultFile.println(statRow.toCSVLine)
  })
  resultFile.close()

}
