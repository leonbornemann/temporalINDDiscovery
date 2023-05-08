package de.hpi.temporal_ind.data.attribute_history.wikipedia

import java.io.File

case class WikipediaColumnHistoryIndex(byBucket: Map[WikipediaPageRange, File]) {

  val byLower = scala.collection.mutable.TreeMap[BigInt, File]() ++ byBucket.map(t => (t._1.lowerInclusive, t._2))

  def getFileForPage(id: BigInt) = {
    val upper = id.+(BigInt(1))
    byLower.maxBefore(upper).get._2
  }
}
