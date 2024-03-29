package de.hpi.temporal_ind.data.attribute_history.statistics

import de.hpi.temporal_ind.data.GLOBAL_CONFIG
import de.hpi.temporal_ind.data.attribute_history.data.original.ColumnHistory
import de.hpi.temporal_ind.data.attribute_history.statistics

import java.io.{File, PrintWriter}

object StatisticsExportMain extends App {
  GLOBAL_CONFIG.setSettingsForDataSource("wikipedia")
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
