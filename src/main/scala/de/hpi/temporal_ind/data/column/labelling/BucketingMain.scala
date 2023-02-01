package de.hpi.temporal_ind.data.column.labelling

import com.typesafe.scalalogging.StrictLogging
import de.hpi.temporal_ind.data
import de.hpi.temporal_ind.data.column
import de.hpi.temporal_ind.data.column.data
import de.hpi.temporal_ind.data.column.data.many.InclusionDependencyFromMany
import de.hpi.temporal_ind.data.column.data.{ColumnHistoryMetadata, original}
import de.hpi.temporal_ind.data.column.data.original.ColumnHistory
import de.hpi.temporal_ind.data.column.statistics.ColumnHistoryStatRow
import de.hpi.temporal_ind.data.wikipedia.GLOBAL_CONFIG

import java.io.{File, PrintWriter}
import scala.io.Source

object BucketingMain extends App with StrictLogging{
  println(s"Called with ${args.toIndexedSeq}")
  val indFile = args(0)
  val metadataFile = args(1)
  val outputDir = args(2)
  GLOBAL_CONFIG.setSettingsForDataSource("wikipedia")
  val buckets = Seq((1,4),(4,16),(16,20000))

  implicit class Crossable[X](xs: Iterable[X]) {
    def cross[Y](ys: Iterable[Y]) = for {x <- xs; y <- ys} yield (x, y)
  }

  val indBuckets = buckets.cross(buckets)
  val bucketFiles = indBuckets.map{case b =>
    val ((lmin,lmax),(rmin,rmax)) = b
    val filename = s"$lmin-${lmax}__$rmin-$rmax.txt"
    (b,new PrintWriter(s"$outputDir/$filename"))
  }.toMap
  var counter = 0
  val metadata = ColumnHistoryMetadata.readAsMap(metadataFile)
  Source
    .fromFile(indFile)
    .getLines()
    .foreach(l => {
      counter+=1
      val ind = InclusionDependencyFromMany.fromManyOutputString(l)
      val left = ind.lhsColumnID
      val right = ind.rhsColumnID
      val (leftBucket,rightBucket) = getBuckets(metadata(left).nChangeVersions,metadata(right).nChangeVersions,buckets)
      bucketFiles((leftBucket,rightBucket)).println(l)
      if(counter%1000000==0)
        logger.debug(s"Finished $counter")
    })
  bucketFiles.foreach(_._2.close())

  def getBuckets(nVersionsWithChangesLeft: Int,nVersionsWithChangesRight: Int, buckets: Seq[(Int, Int)]) = {
    val leftBucket = buckets
      .find{case (min,max) => nVersionsWithChangesLeft>=min && nVersionsWithChangesLeft<=max}
      .get
    val rightBucket = buckets
      .find { case (min, max) => nVersionsWithChangesRight >= min && nVersionsWithChangesRight <= max }
      .get
    (leftBucket,rightBucket)
  }


}
