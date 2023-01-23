package de.hpi.temporal_ind.data.column.labelling

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
  val buckets = Seq((1,2),(2,4),(4,8),(8,16),(16,32),(32,20000))
  val curFiles = collection.mutable.HashMap[String,PrintWriter]()
  val bucketSizes = collection.mutable.HashMap[String,Int]()
  def getBucket(nVersionsWithChanges: Int, buckets: Seq[(Int, Int)]) =
    buckets
      .find{case (min,max) => nVersionsWithChanges>=min && nVersionsWithChanges<=max}
      .get

  def getDirName(t: (Int, Int)) = {
    val (min,max) = t
    s"${min}_$max"
  }

  def appendToCurrentFile(ch: ColumnHistory, bucketName: String,originFile:File) = {
    val filePath = outputDir + "/" + bucketName + "/" + originFile
    val writer = curFiles(filePath)
    bucketSizes.put(bucketName,bucketSizes.getOrElse(bucketName,0))
    ch.appendToWriter(writer)
  }

  iterators
    .flatMap{case (f,chs) => chs.map(ch => (ch,f))}
    .map{case (ch,f) => (ch,ColumnHistoryStatRow(ch),f)}
    .withFilter{case (_,stats,_) => stats.sizeStatistics.median >= 2 && stats.lifetimeInDays >= 30 && stats.nVersionsWithChanges>0}
    .map { case (ch,stats,f) => (ch,getDirName(getBucket(stats.nVersionsWithChanges,buckets)),f)}
    .foreach{ case (ch,dirName,originFile) => appendToCurrentFile(ch,dirName,originFile)}
  curFiles.values.foreach(_.close())
  bucketSizes
    .toIndexedSeq
    .sortBy(_._1)
    .foreach(println(_))
}
