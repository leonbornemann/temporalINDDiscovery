package de.hpi.temporal_ind.data.attribute_history.labelling

import com.typesafe.scalalogging.StrictLogging
import de.hpi.temporal_ind.data.GLOBAL_CONFIG
import de.hpi.temporal_ind.data.attribute_history.data.metadata.ColumnHistoryMetadata
import de.hpi.temporal_ind.data.attribute_history.data.original.ColumnHistory

import java.io.{File, PrintWriter}

object ColumnBucketingMain extends App with StrictLogging{
  println(s"Called with ${args.toIndexedSeq}")
  val colHistoryDir = args(0)
  val metadataFile = args(1)
  val outputDir = args(2)
  val minMedianSize = args(3).toDouble
  GLOBAL_CONFIG.setSettingsForDataSource("wikipedia")
  val buckets = Seq((1, 4), (4, 16), (16, 20000))
  val bucketToDirs = buckets.map{case (min, max) =>
      val dirName = s"$min-${max}"
      val dir = new File(s"$outputDir/$dirName")
      dir.mkdirs()
      ((min,max),dir)
  }.toMap
  val bucketFiles = bucketToDirs.map { case ((min,max),dir) =>
    (dir, collection.mutable.HashMap[String,PrintWriter]())
  }
  var counter = 0
  val metadata = ColumnHistoryMetadata.readAsMap(metadataFile)
  new File(colHistoryDir)
    .listFiles()
    .foreach(f => {
      logger.debug(s"Processing ${f.getAbsolutePath}")
      ColumnHistory.iterableFromJsonObjectPerLineFile(f.getAbsolutePath)
        .withFilter(ch => {
          val a = metadata.get(ch.id)
          if(!a.isDefined){
            if(ch.versionsWithNonDeleteChanges.size >= 1){
              println("Weird ",ch.versionsWithNonDeleteChanges.size,ch.id)
            }
            assert(ch.versionsWithNonDeleteChanges.size < 1)
          }
          !a.isEmpty && metadata(ch.id).medianSize>=minMedianSize
        })
        .foreach(ch => {
          val bucket: (Int, Int) = getBucket(metadata(ch.id).nChangeVersions, buckets)
          val dir = bucketToDirs(bucket)
          val pr = bucketFiles(dir).getOrElseUpdate(f.getName, (new PrintWriter(s"${dir.getAbsolutePath}/${f.getName}")))
          ch.appendToWriter(pr)

        })
    })

  bucketFiles.values.foreach(_.values.foreach(_.close()))

  def getBucket(nVersions: Int, buckets: Seq[(Int, Int)]) = {
    buckets
      .find { case (min, max) => nVersions >= min && nVersions <= max }
      .get
  }
}
