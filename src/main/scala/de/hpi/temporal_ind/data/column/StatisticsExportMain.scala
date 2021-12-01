package de.hpi.temporal_ind.data.column

import java.io.{File, PrintWriter}

object StatisticsExportMain extends App {
  val inputDir = new File(args(0))
  val resultFile = new PrintWriter(args(1))
  val iterable = ColumnHistory.iterableFromJsonObjectPerLineDir(inputDir,true)
  resultFile.println(ColumnHistoryStatRow.getSchema.mkString(","))
  iterable.foreach(ch => {
    val statRow = ColumnHistoryStatRow(ch)
    resultFile.println(statRow.toCSVLine)
  })
  resultFile.close()

}
