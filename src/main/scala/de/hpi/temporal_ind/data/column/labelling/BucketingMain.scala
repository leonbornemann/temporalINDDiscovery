package de.hpi.temporal_ind.data.column.labelling

import de.hpi.temporal_ind.data
import de.hpi.temporal_ind.data.column
import de.hpi.temporal_ind.data.column.data
import de.hpi.temporal_ind.data.column.data.original
import de.hpi.temporal_ind.data.column.data.original.ColumnHistory
import de.hpi.temporal_ind.data.column.statistics.ColumnHistoryStatRow
import de.hpi.temporal_ind.data.wikipedia.GLOBAL_CONFIG

import java.io.{File, PrintWriter}

object BucketingMain extends App {
  println(s"Called with ${args.toIndexedSeq}")
  val inputDir = args(0)
  val outputDir = args(1)
  GLOBAL_CONFIG.setSettingsForDataSource("wikipedia")
  val iterators = new File(inputDir)
    .listFiles()
    .map(f => (f,ColumnHistory.iterableFromJsonObjectPerLineFile(f.getAbsolutePath,true)))
  val buckets = Seq((1,4),(4,16),(16,20000))
  val bucketSizes = collection.mutable.HashMap[String,Int]()
  def getBucket(nVersionsWithChanges: Int, buckets: Seq[(Int, Int)]) =
    buckets
      .find{case (min,max) => nVersionsWithChanges>=min && nVersionsWithChanges<=max}
      .get

  def getDirName(t: (Int, Int)) = {
    val (min,max) = t
    s"${min}_$max"
  }

  def appendToCurrentFile(curFiles:collection.mutable.HashMap[String,PrintWriter],ch: ColumnHistory, bucketName: String,originFile:File) = {
    val newDir = new File(outputDir + "/" + bucketName)
    if(!newDir.exists()){
      println(s"Creating new Dir $newDir")
    }
    newDir.mkdirs()
    val filePath = outputDir + "/" + bucketName + "/" + originFile.getName
    val writer = curFiles.getOrElseUpdate(filePath, new PrintWriter(filePath))
    bucketSizes.put(bucketName,bucketSizes.getOrElse(bucketName,0))
    ch.appendToWriter(writer)
  }

  def process(f: File, chs: ColumnHistory.JsonObjectPerLineFileIterator) = {
    val curFiles = collection.mutable.HashMap[String,PrintWriter]()
    chs
      .map { case (ch) => {
        (ch, ColumnHistoryStatRow(ch), f)
      }}
      .withFilter { case (_, stats, _) => {
        stats.satisfiesBasicFilter
      }
      }
      .map { case (ch, stats, f) => (ch, getDirName(getBucket(stats.nVersionsWithChanges, buckets)), f) }
      .foreach { case (ch, dirName, originFile) => appendToCurrentFile(curFiles,ch, dirName, originFile) }
    curFiles.values.foreach(_.close())
  }

  iterators
    .foreach{case (f,chs) => process(f,chs)}

  bucketSizes
    .toIndexedSeq
    .sortBy(_._1)
    .foreach(println(_))
}
