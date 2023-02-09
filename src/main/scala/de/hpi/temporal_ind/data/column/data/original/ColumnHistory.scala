package de.hpi.temporal_ind.data.column.data.original

import com.google.zetasketch.HyperLogLogPlusPlus
import de.hpi.temporal_ind.data.column.data.AbstractColumnVersion
import de.hpi.temporal_ind.data.column.data.encoded.ColumnHistoryEncoded
import de.hpi.temporal_ind.data.column.io.Dictionary
import de.hpi.temporal_ind.data.column.statistics.ValueSequenceStatistics
import de.hpi.temporal_ind.data.column.wikipedia.{WikipediaColumnHistoryIndex, WikipediaPageRange}
import de.hpi.temporal_ind.data.{JsonReadable, JsonWritable}

import java.io.File
import java.time.Instant
import java.time.temporal.ChronoUnit
import scala.collection.mutable.ArrayBuffer

case class ColumnHistory(id: String,
                         tableId: String,
                         pageID: String,
                         pageTitle: String,
                         columnVersions: ArrayBuffer[ColumnVersion]
                        ) extends JsonWritable[ColumnHistory]{

  def hyperLogLogOfUnionedValueSet = {
    val hll:HyperLogLogPlusPlus[String] = new HyperLogLogPlusPlus.Builder().buildForStrings();
    columnVersions
      .flatMap(cv => cv.values)
      .toSet
      .foreach((s:String) => hll.add(s))
    hll
  }

  def hyperLogLogOverlapOfUnionedValuesets(b: ColumnHistory) = {
    val myLogLog = hyperLogLogOfUnionedValueSet
    val myLogLogOther = b.hyperLogLogOfUnionedValueSet
    val sizeA = myLogLog.result()
    val sizeB = myLogLogOther.result()
    myLogLog.merge(myLogLogOther)
    val unionSize = myLogLog.result()
    val intersection = sizeA+sizeB-unionSize
    intersection
  }

  def overlapOfUnionedValuesets(b:ColumnHistory) = {
    val mySet = columnVersions
      .flatMap(cv => cv.values)
      .toSet
    val otherSet = b.columnVersions
      .flatMap(cv => cv.values)
      .toSet
    mySet.intersect(otherSet).size
  }

  def medianSize = {
    val valueSetSizes = versionsWithNonDeleteChanges.map(_.values.size)
    ValueSequenceStatistics(valueSetSizes.map(_.toDouble)).median
  }


  def existsNonDeleteInVersionRange(beginTimestamp:Instant,endTimestampExclusive:Instant) = {
    val beginVersion = versionAt(beginTimestamp)
    if(beginVersion.isDelete) {
      columnVersions.exists{v => v.timestamp.isAfter(beginTimestamp) &&
        v.timestamp.isBefore(endTimestampExclusive) &&
        !v.isDelete}
    } else
      true
  }

  def versionUnion(beginTimestamp: Instant, endTimestampExclusive: Instant): ColumnVersion = {
    val beginVersion = versionAt(beginTimestamp)
    val otherVersions = columnVersions.filter(v => v.timestamp.isAfter(beginTimestamp) && v.timestamp.isBefore(endTimestampExclusive))
    val unionedValues = beginVersion
      .values
      .union(otherVersions.flatMap(_.values).toSet)
    val lastNonDeleteIndex = otherVersions.lastIndexWhere(!_.isDelete)
    val latestNonDeleteRevisionVersion = if(lastNonDeleteIndex== -1) beginVersion else otherVersions(lastNonDeleteIndex)
    ColumnVersion(latestNonDeleteRevisionVersion.revisionID,
      latestNonDeleteRevisionVersion.revisionDate,
      unionedValues,
      latestNonDeleteRevisionVersion.header,
      latestNonDeleteRevisionVersion.position,
      latestNonDeleteRevisionVersion.columnNotPresent)
  }


  //def transformValueset(f:(Set[String]) => Set[String]) = {
  //    ColumnHistory(id,tableId,pageID,pageTitle,columnVersions.map(cv => ColumnVersion(cv.revisionID,cv.revisionDate,f(cv.values),cv.columnNotPresent)))
  //  }
  def transformHeader(f: Set[String] => Set[String]) = {
    ColumnHistory(id,tableId,pageID,pageTitle,columnVersions.map{cv =>
      val newHeader = if(cv.header.isEmpty) cv.header else Some(f(Set(cv.header.get)).head)
      ColumnVersion(cv.revisionID,cv.revisionDate,cv.values,newHeader,cv.position,cv.columnNotPresent)
    })
  }



  def applyDictionary(dict:Dictionary) = {
    ColumnHistoryEncoded(id,tableId, pageID, pageTitle, columnVersions.map(cv => cv.applyDictionary(dict)))
  }

  def asOrderedHistory = new OrderedColumnHistory(id,
    tableId,
    pageID,
    pageTitle,
    new OrderedColumnVersionList(collection.mutable.TreeMap[Instant,ColumnVersion]() ++ columnVersions.map(cv => (cv.timestamp,cv))))


  def firstInsertToLastDeleteTimeInDays(endTimeData: Instant) = {
    if(columnVersions.last.isDelete)
      ChronoUnit.DAYS.between(columnVersions.head.timestamp,columnVersions.last.timestamp)
    else
      ChronoUnit.DAYS.between(columnVersions.head.timestamp,endTimeData)
  }

  def totalLifeTimeInDays(endTimeData: Instant) = {
    val withIndex = columnVersions
      .zipWithIndex
    withIndex.map{case (cv,i) => {
      if(!cv.isDelete){
        if(i!=withIndex.size-1)
          ChronoUnit.SECONDS.between(cv.timestamp,withIndex(i+1)._1.timestamp)
        else
          ChronoUnit.SECONDS.between(cv.timestamp,endTimeData)
      } else {
        0
      }
    } }
      .sum / (60*60*24)
  }

  def versionsWithNonDeleteChanges = {
//    if(columnVersions.exists(_.isDelete) || columnVersions.exists(_.values.isEmpty)){
//      println()
//    }
    val withOutDeletes = columnVersions
      .filter(!_.isDelete)
      .zipWithIndex
    withOutDeletes
      .withFilter(t => t._2 != 0 && t._1.values != withOutDeletes(t._2-1)._1.values)
      .map(_._1)
  }


  def columnVersionsSorted: Boolean = {
    val withIndex = columnVersions
      .zipWithIndex
    withIndex
      .forall(t => t._2==0 || columnVersions(t._2-1).timestamp.isBefore(t._1.timestamp))
  }

  def versionAt(inputTimestamp: Instant):ColumnVersion = {
    assert(columnVersionsSorted)
    if(columnVersions.head.timestamp.isAfter(inputTimestamp))
      ColumnVersion.COLUMN_DELETE(AbstractColumnVersion.INITIALEMPTYID,inputTimestamp.toString)
    else {
      val withIndex = columnVersions
        .zipWithIndex
      withIndex
        .find { case (cv, i) => !cv.timestamp.isAfter(inputTimestamp) &&
          (i == withIndex.size - 1 || withIndex(i + 1)._1.timestamp.isAfter(inputTimestamp))
        }
        .get._1
    }
  }


  def versionsWithAliveTime(lastDate:Instant) = {
    withAliveTime(columnVersions,lastDate)
  }

  private def withAliveTime(versions:ArrayBuffer[ColumnVersion],lastDate:Instant) = {
    val withIndex = versions
      .zipWithIndex
    withIndex
      .map{case (cv,i)=> {
        if(i==columnVersions.size-1)
          (cv,ChronoUnit.SECONDS.between(cv.timestamp,lastDate))
        else
          (cv,ChronoUnit.SECONDS.between(cv.timestamp,withIndex(i+1)._1.timestamp))
      }}
  }

  def nonDeleteVersionsWithAliveTime(lastDate:Instant) = {
    withAliveTime(columnVersions,lastDate)
      .filter(!_._1.isDelete)
  }


  def transformValueset(f:(Set[String]) => Set[String]) = {
    ColumnHistory(id,tableId,pageID,pageTitle,columnVersions.map(cv => ColumnVersion(cv.revisionID,cv.revisionDate,f(cv.values),cv.header,cv.position,cv.columnNotPresent)))
  }

  def prependEmptyVersionIfNotPResent(startTime:Instant) = {
    assert(!startTime.isBefore(columnVersions.head.timestamp))
    if(columnVersions.head.timestamp != startTime) {
      ColumnHistory(id,tableId,pageID,pageTitle,ArrayBuffer(ColumnVersion.COLUMN_DELETE(AbstractColumnVersion.INITIALEMPTYID,startTime.toString)) ++ columnVersions)
    } else {
      this
    }
  }
}
object ColumnHistory extends JsonReadable[ColumnHistory] {

  def getIndexForFilesInDir(file:File) = {
    val byBucket = file.listFiles
      .map(f => {
        val tokens = f.getName.split("_")(0)
          .split("xml-p")(1)
          .split("p")
        val lowerID=tokens(0)
        val upperID=tokens(1)
        (WikipediaPageRange(BigInt(lowerID),BigInt(upperID)),f)
      })
      .toMap
    WikipediaColumnHistoryIndex(byBucket)
  }

}
