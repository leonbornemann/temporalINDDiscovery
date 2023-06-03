package de.hpi.temporal_ind.oneshot

import de.hpi.temporal_ind.data.GLOBAL_CONFIG
import de.hpi.temporal_ind.data.attribute_history.data.file_search.IndexedColumnHistories
import de.hpi.temporal_ind.data.attribute_history.data.many.InclusionDependencyFromMany

import java.io.{File, PrintWriter}

object SingleColHistoryINDsDebug extends App {
  GLOBAL_CONFIG.setSettingsForDataSource("wikipedia")
  val dir = new File("/home/leon/data/temporalINDDiscovery/wikipedia/debugHistories/")
  val pageID = 30510135L
  val colID = "8664c841-3f85-4d21-812f-cb0a2cf8fab7"
  val index = IndexedColumnHistories.loadForPageIDS(dir, IndexedSeq(pageID))
  val history = index.multiLevelIndex(pageID.toString)(colID)
  val file = IndexedColumnHistories.getFileForID(new File("/home/leon/data/temporalINDDiscovery/wikipedia/filteredHistories/"),pageID)
  println(file)
  println(history)
}