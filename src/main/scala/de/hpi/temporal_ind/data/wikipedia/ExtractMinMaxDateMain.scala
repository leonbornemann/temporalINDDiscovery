package de.hpi.temporal_ind.data.wikipedia

import java.io.File
import java.time.Instant

object ExtractMinMaxDateMain extends App {
  GLOBAL_CONFIG.setSettingsForDataSource(args(0))
  val inputDir = new File(args(1))
  val allTHs = TableHistory.iterableFromJsonObjectPerLineDir(inputDir,true)
  var minTimestamp:Option[Instant] = None
  var maxTimestamp:Option[Instant] = None
  allTHs.foreach(th => {
    th.tables.foreach(tv => {
      if(!minTimestamp.isDefined || tv.timestamp.isBefore(minTimestamp.get))
        minTimestamp = Some(tv.timestamp)
      if(!maxTimestamp.isDefined || tv.timestamp.isAfter(maxTimestamp.get))
        maxTimestamp = Some(tv.timestamp)
    })
  })
  println(minTimestamp,maxTimestamp)
}
