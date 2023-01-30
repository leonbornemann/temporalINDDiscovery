package de.hpi.temporal_ind.data.column.data

import de.hpi.temporal_ind.data.column.data.original.ColumnHistory
import de.hpi.temporal_ind.data.{JsonReadable, JsonWritable}

import java.io.File
import java.time.Instant

case class UniquenessMap(uniquenessMap:collection.Map[String,collection.Map[Instant,Boolean]]) extends JsonWritable[UniquenessMap] {

}
object UniquenessMap extends JsonReadable[UniquenessMap] {

  def createForDir(dir: File): UniquenessMap = {
    val map = collection.mutable.HashMap[String,collection.mutable.HashMap[Instant,Boolean]]()
    dir
      .listFiles()
      .foreach(f => {
        val histories = ColumnHistory.fromJsonObjectPerLineFile(f.getAbsolutePath)
          .groupBy(_.tableId)
          .foreach{case (tID,chs) => {
            val allVersions = chs.flatMap(_.columnVersions.map(_.timestamp)).toSet.toIndexedSeq.sorted
            allVersions.foreach(v => {
              val sizes = chs.map(ch => (ch.id,ch.versionAt(v).values.size)).toMap
              val maxSize = sizes.values.max
              val uniquenessAtV = sizes.map(s => (s._1,s._2==maxSize))
              uniquenessAtV.map{case (id,isUnique) => {
                val mapThisVersion = map.getOrElseUpdate(id,collection.mutable.HashMap[Instant,Boolean]())
                mapThisVersion.put(v,isUnique)
              }}
            })
          }}
      })
    UniquenessMap(map)
  }
}
