package de.hpi.temporal_ind.data.attribute_history.data.metadata

import de.hpi.temporal_ind.data.attribute_history.data.original.ColumnHistory
import de.hpi.temporal_ind.data.{JsonReadable, JsonWritable}

import java.io.{File, PrintWriter}
import java.time.Instant

case class ColumnHistoryMetadata(id:String,uniqueAtLatestTimestamp:Boolean, nChangeVersions:Int,medianSize:Double) extends JsonWritable[ColumnHistoryMetadata]{

}

object ColumnHistoryMetadata extends JsonReadable[ColumnHistoryMetadata]{
  def readAsMap(metadataFile: String) = {
    fromJsonObjectPerLineFile(metadataFile)
      .map(md => (md.id,md))
      .toMap
  }


  def extractAndSerialize(inputDir: File, outFile: File, timestamp: Instant) = {
    val pr = new PrintWriter(outFile)
    createForDir(inputDir, pr, timestamp)
    pr.close()
  }

  def createForDir(dir: File, pr: PrintWriter, timestamp: Instant) = {
    dir
      .listFiles()
      .foreach(f => {
        println(s"Processing $f")
        ColumnHistory
          .fromJsonObjectPerLineFile(f.getAbsolutePath)
          .groupBy(_.tableId)
          .foreach { case (tID, chs) => {
            val sizes = chs.map(ch => ch.versionAt(timestamp).values.size).toIndexedSeq
            val maxSize = sizes.max
            val uniquenessAtV = sizes.map(s => s == maxSize)
            val nChangeVersions = chs.toIndexedSeq.map(ch => ch.versionsWithNonDeleteChanges.size)
            val medianSizes = chs.toIndexedSeq.map(ch => ch.medianSize)
            (0 until chs.size)
              .withFilter(i => nChangeVersions(i)>0)
              .foreach(i => ColumnHistoryMetadata(chs(i).id, uniquenessAtV(i), nChangeVersions(i),medianSizes(i)).appendToWriter(pr))
          }
          }
      })
  }

}
