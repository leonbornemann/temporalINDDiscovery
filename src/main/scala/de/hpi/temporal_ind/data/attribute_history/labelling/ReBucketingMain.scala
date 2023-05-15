package de.hpi.temporal_ind.data.attribute_history.labelling

import de.hpi.temporal_ind.data.attribute_history.data.metadata.ColumnHistoryMetadata

import java.io.{File, PrintWriter}
import scala.io.Source

object ReBucketingMain extends App {
  val labelledDir = new File(args(0))
  val outputDir = new File(args(1))

  val buckets = Seq((4, 8), (8, 16), (16, 20000))

  implicit class Crossable[X](xs: Iterable[X]) {
    def cross[Y](ys: Iterable[Y]) = for {x <- xs; y <- ys} yield (x, y)
  }

  val indBuckets = buckets.cross(buckets)
  val header = "versionURLFK,versionURLPK,pageTitleFK,colHeaderFK,colPositionFK,pageTitlePK,colHeaderPK,colPositionPK,isGenuine,fkTableID,pkTableID,fkValues,pkValues,fkID,pkID,fkPageID,pkPageID,fkTitle,pkTitle"
  val bucketFiles = indBuckets.map { case b =>
    val ((lmin, lmax), (rmin, rmax)) = b
    val filename = s"$lmin-${lmax}__$rmin-$rmax.txt"
    (b, new PrintWriter(s"$outputDir/$filename"))
  }.toMap
  bucketFiles.foreach(_._2.println(header))
  val metadata = ColumnHistoryMetadata.readAsMap(args(2))
  labelledDir.listFiles().foreach(f => {
    val lines = Source.fromFile(f)
      .getLines()
      .toIndexedSeq
    val linesWithContent = lines
      .tail
    val header = lines.head
    linesWithContent.foreach(l => {
      val leftID = l.split(",")(13)
      val rightID = l.split(",")(14)
      val (leftBucket, rightBucket) = getBuckets(metadata(leftID).nChangeVersions, metadata(rightID).nChangeVersions, buckets)
      bucketFiles((leftBucket, rightBucket)).println(l)
    })
  })
  bucketFiles.foreach(_._2.close())




  def getBuckets(nVersionsWithChangesLeft: Int, nVersionsWithChangesRight: Int, buckets: Seq[(Int, Int)]) = {
    val leftBucket = buckets
      .find { case (min, max) => nVersionsWithChangesLeft >= min && nVersionsWithChangesLeft <= max }
      .get
    val rightBucket = buckets
      .find { case (min, max) => nVersionsWithChangesRight >= min && nVersionsWithChangesRight <= max }
      .get
    (leftBucket, rightBucket)
  }


}
