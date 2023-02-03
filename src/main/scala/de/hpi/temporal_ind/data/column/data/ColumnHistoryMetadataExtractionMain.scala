package de.hpi.temporal_ind.data.column.data

import de.hpi.temporal_ind.data.wikipedia.GLOBAL_CONFIG

import java.io.File
import java.time.Instant

object ColumnHistoryMetadataExtractionMain extends App {
  GLOBAL_CONFIG.setSettingsForDataSource("wikipedia")
  val inputDir = args(0)
  val outputFile = args(1)
  val timestamp = Instant.parse(args(2))
  ColumnHistoryMetadata
    .extractAndSerialize(new File(inputDir),new File(outputFile),timestamp)
  //test Reading:
  println("found",ColumnHistoryMetadata.fromJsonObjectPerLineFile(outputFile).size,"metadata objects")

}
