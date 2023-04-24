package de.hpi.temporal_ind.oneshot

import de.hpi.temporal_ind.data.column.data.IndexedColumnHistories
import de.hpi.temporal_ind.data.wikipedia.GLOBAL_CONFIG

import java.io.{File, PrintWriter}

object SingleColumnHistoryInspect extends App {
  GLOBAL_CONFIG.setSettingsForDataSource("wikipedia")
  val dir = new File("/home/leon/data/temporalINDDiscovery/wikipedia/filteredBuckets/4-16/")
  val pageID = 39262760L
  val columnID = "4b186a64-983f-4671-8843-5f2097ba0e9b"
  val index = IndexedColumnHistories.loadForPageIDS(dir, IndexedSeq(pageID))
  val res = index.multiLevelIndex(pageID.toString)(columnID)
  println(res)
}
