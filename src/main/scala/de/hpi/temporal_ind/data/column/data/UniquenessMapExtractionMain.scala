package de.hpi.temporal_ind.data.column.data

import de.hpi.temporal_ind.data.wikipedia.GLOBAL_CONFIG

import java.io.File

object UniquenessMapExtractionMain extends App {
  GLOBAL_CONFIG.setSettingsForDataSource("wikipedia")
  val inputDir = args(0)
  val outputFile = args(1)
  UniquenessMap.createForDir(new File(inputDir))
    .toJsonFile(new File(outputFile))

}
