package de.hpi.temporal_ind.oneshot

import de.hpi.temporal_ind.data.column.data.original.ColumnHistory
import de.hpi.temporal_ind.data.wikipedia.{GLOBAL_CONFIG, WikipediaDataPreparer}
import de.hpi.temporal_ind.discovery.input_data.InputDataManager

import java.io.File

object ImprovedNumericalFilterMain extends App {
  println("called with",args.toIndexedSeq)
  GLOBAL_CONFIG.setSettingsForDataSource("wikipedia")
  val dm = new InputDataManager(args(0))
  val outputFile = args(1)
  val preparer = new WikipediaDataPreparer()
  val histories = dm.loadData()
  println(s"Found ${histories.size} histories before filter")
  val filtered = histories.filter(ch => !preparer.mostlyNumeric(ch.toColumnHistory))
  //val filteredReverse = histories.filter(ch => preparer.mostlyNumeric(ch.toColumnHistory))
  println(s"Found ${filtered.size} histories after filter")
  dm.serializeAsBinary(filtered,outputFile)

}
