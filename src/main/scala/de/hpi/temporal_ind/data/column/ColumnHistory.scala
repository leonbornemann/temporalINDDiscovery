package de.hpi.temporal_ind.data.column

import de.hpi.temporal_ind.data.{JsonReadable, JsonWritable}

import java.time.temporal.ChronoUnit
import java.time.{Duration, Instant}
import scala.collection.mutable.ArrayBuffer

case class ColumnHistory(id: String,
                         tableId: String,
                         pageID: String,
                         pageTitle: String,
                         columnVersions: ArrayBuffer[ColumnVersion]
                        ) extends JsonWritable[ColumnHistory]{

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
      ColumnVersion.COLUMN_DELETE(ColumnVersion.INITIALEMPTYID,inputTimestamp.toString)
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
    val withIndex = columnVersions
      .zipWithIndex
    withIndex
      .map{case (cv,i)=> {
        if(i==columnVersions.size-1)
          (cv,ChronoUnit.SECONDS.between(cv.timestamp,lastDate))
        else
          (cv,ChronoUnit.SECONDS.between(cv.timestamp,withIndex(i+1)._1.timestamp))
      }}
  }


  def transformValueset(f:(Set[String]) => Set[String]) = {
    ColumnHistory(id,tableId,pageID,pageTitle,columnVersions.map(cv => ColumnVersion(cv.revisionID,cv.revisionDate,f(cv.values),cv.columnNotPresent)))
  }

  def prependEmptyVersionIfNotPResent(startTime:Instant) = {
    assert(!startTime.isBefore(columnVersions.head.timestamp))
    if(columnVersions.head.timestamp != startTime) {
      ColumnHistory(id,tableId,pageID,pageTitle,ArrayBuffer(ColumnVersion.COLUMN_DELETE(ColumnVersion.INITIALEMPTYID,startTime.toString)) ++ columnVersions)
    } else {
      this
    }
  }
}
object ColumnHistory extends JsonReadable[ColumnHistory]
