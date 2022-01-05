package de.hpi.temporal_ind.data.column.data.encoded

import de.hpi.temporal_ind.data.column.data.original.{ColumnVersion, OrderedColumnHistory, OrderedColumnVersionList}
import de.hpi.temporal_ind.data.{JsonReadable, JsonWritable}

import java.io.File
import java.time.Instant
import scala.collection.mutable.ArrayBuffer

case class ColumnHistoryEncoded(id: String, tableId: String, pageID: String, pageTitle: String, value: ArrayBuffer[ColumnVersionEncoded]) extends JsonWritable[ColumnHistoryEncoded] {

  def asOrderedVersionMap = new OrderedEncodedColumnHistory(id,
    tableId,
    pageID,
    pageTitle,
    new OrderedEncodedColumnVersionList(collection.mutable.TreeMap[Instant,ColumnVersionEncoded]() ++ value.map(cv => (cv.timestamp,cv))))


}
object ColumnHistoryEncoded extends JsonReadable[ColumnHistoryEncoded] {

  def loadIntoMap(dir: File) = {
    val map = collection.mutable.TreeMap[String,ColumnHistoryEncoded]()
    var count = 0
    val files = dir.listFiles()
    files.foreach { f =>
      count +=1
      println(s"Reading ${f.getName} ( $count / ${files.size})")
      val histories = ColumnHistoryEncoded.fromJsonObjectPerLineFile(f.getAbsolutePath)
      histories.foreach(h => {
        map.put(h.id,h)
      })
    }
    map
  }

}
