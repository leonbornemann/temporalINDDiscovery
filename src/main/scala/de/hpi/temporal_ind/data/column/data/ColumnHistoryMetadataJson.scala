package de.hpi.temporal_ind.data.column.data

import de.hpi.temporal_ind.data.{JsonReadable, JsonWritable}
import de.hpi.temporal_ind.data.column.data.original.ColumnHistory
import de.hpi.temporal_ind.data.column.statistics.ColumnHistoryStatRow

import java.io.{File, PrintWriter}
import java.time.Instant

case class ColumnHistoryMetadataJson(id:String,uniqueness:collection.Map[String,Boolean], nChangeVersions:Int) extends JsonWritable[ColumnHistoryMetadataJson]{

  def toColumnHistoryMetadata = ColumnHistoryMetadata(id,uniqueness.map{case (k,v) => (Instant.parse(k),v)},nChangeVersions)

}

object ColumnHistoryMetadataJson extends JsonReadable[ColumnHistoryMetadataJson] {
  def extractAndSerialize(inputDir: File, outFile: File) = {
    val pr = new PrintWriter(outFile)
    createForDir(inputDir,pr)
    pr.close()
  }


  def createForDir(dir: File,pr:PrintWriter)= {
    dir
      .listFiles()
      .foreach(f => {
        println(s"Processing $f")
        val uniqueness = collection.mutable.HashMap[String, collection.mutable.HashMap[Instant, Boolean]]()
        val nChangeVersions = collection.mutable.HashMap[String, Int]()
        ColumnHistory.fromJsonObjectPerLineFile(f.getAbsolutePath)
          .groupBy(_.tableId)
          .foreach { case (tID, chs) => {
            val allVersions = chs.flatMap(_.columnVersions.map(_.timestamp)).toSet.toIndexedSeq.sorted
            chs.foreach(ch => {
              nChangeVersions.put(ch.id, ColumnHistoryStatRow(ch).nVersionsWithChanges)
            })
            allVersions.foreach(v => {
              val sizes = chs.map(ch => (ch.id, ch.versionAt(v).values.size)).toMap
              val maxSize = sizes.values.max
              val uniquenessAtV = sizes.map(s => (s._1, s._2 == maxSize))
              uniquenessAtV.map { case (id, isUnique) => {
                val mapThisVersion = uniqueness.getOrElseUpdate(id, collection.mutable.HashMap[Instant, Boolean]())
                mapThisVersion.put(v, isUnique)
              }
              }
            })
          }
          }
        val finalMap = uniqueness
          .keySet
          .toIndexedSeq
          .map(k => (k, ColumnHistoryMetadata(k, uniqueness(k), nChangeVersions(k))))
          .toMap
        finalMap.values.foreach(md => md.toSerialized.appendToWriter(pr))
      })

  }

}
