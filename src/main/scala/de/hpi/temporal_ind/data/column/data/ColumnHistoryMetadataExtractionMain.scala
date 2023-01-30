package de.hpi.temporal_ind.data.column.data

import de.hpi.temporal_ind.data.wikipedia.GLOBAL_CONFIG

import java.io.File

object ColumnHistoryMetadataExtractionMain extends App {
  GLOBAL_CONFIG.setSettingsForDataSource("wikipedia")
  val inputDir = args(0)
  val outputFile = args(1)
  ColumnHistoryMetadataJson
    .extractAndSerialize(new File(inputDir),new File(outputFile))
  //test Reading:
  println("found",ColumnHistoryMetadata.readFromJsonObjectPerLineFile(outputFile).size,"metadata objects")

}
