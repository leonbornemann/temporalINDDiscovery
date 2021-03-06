package de.hpi.temporal_ind.data.column.data.original

import de.hpi.temporal_ind.data.column.data.AbstractColumnVersion
import de.hpi.temporal_ind.data.column.data.encoded.ColumnHistoryEncoded
import de.hpi.temporal_ind.data.column.io.Dictionary
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

  def numericTimeShare = {

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
    ColumnHistory(id,tableId,pageID,pageTitle,columnVersions.map(cv => ColumnVersion(cv.revisionID,cv.revisionDate,f(cv.values),cv.columnNotPresent)))
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
