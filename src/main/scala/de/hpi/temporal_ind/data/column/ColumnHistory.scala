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
    ColumnHistory(id,tableId,pageID,pageTitle,columnVersions.map(cv => ColumnVersion(cv.revisionID,cv.revisionDate,f(cv.values))))
  }

  def prependEmptyVersionIfNotPResent(startTime:Instant) = {
    assert(!startTime.isBefore(columnVersions.head.timestamp))
    if(columnVersions.head.timestamp != startTime) {
      ColumnHistory(id,tableId,pageID,pageTitle,ArrayBuffer(ColumnVersion.EMPTY(ColumnVersion.INITIALEMPTYID,startTime.toString)) ++ columnVersions)
    } else {
      this
    }
  }
}
object ColumnHistory extends JsonReadable[ColumnHistory]
