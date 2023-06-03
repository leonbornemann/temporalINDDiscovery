package de.hpi.temporal_ind.data.attribute_history.data.original

import de.hpi.temporal_ind.data.GLOBAL_CONFIG
import de.hpi.temporal_ind.data.attribute_history.data.{AbstractColumnVersion, AbstractOrderedColumnHistory, ColumnHistoryID}
import de.hpi.temporal_ind.data.column.data.original.{KryoSerializableColumnHistory, KryoSerializableColumnVersion}
import de.hpi.temporal_ind.discovery.TINDParameters
import de.hpi.temporal_ind.discovery.statistics_and_results.TimeSliceStats

import java.io.File
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util
import scala.collection.mutable
import scala.jdk.CollectionConverters.{ListHasAsScala, SeqHasAsJava}

class OrderedColumnHistory(val id: String,
                           val tableId: String,
                           val pageID: String,
                           val pageTitle: String,
                           val history: OrderedColumnVersionList) extends AbstractOrderedColumnHistory[String] with Serializable{
  def lifetimeIsBelowThreshold(queryParameters: TINDParameters): Boolean = {
    val withIndex = history.versionsSorted
      .zipWithIndex
    val weights = withIndex.map { case (cv, i) => {
      if (!cv.isDelete) {
        val end = if (i != withIndex.size - 1)
          withIndex(i + 1)._1.timestamp
        else {
          GLOBAL_CONFIG.lastInstant
        }
        queryParameters.omega.weight(cv.timestamp,end)
      } else {
        0
      }
    }
    }
    weights.sum < queryParameters.absoluteEpsilon
  }

  def columnHistoryID: ColumnHistoryID = ColumnHistoryID(pageID, tableId, id)

  def toColumnHistory = {
    ColumnHistory(id , tableId , pageID, pageTitle , columnVersions = collection.mutable.ArrayBuffer() ++ history.versions.toIndexedSeq.map(_._2.asInstanceOf[ColumnVersion]))
  }

  def extractStatsForTimeRange(timeRange:(Instant,Instant),stats:TimeSliceStats) = {
    val begin = if(history.versions.contains(timeRange._1)) timeRange._1 else history.versions.maxBefore(timeRange._1).map(_._1).getOrElse(GLOBAL_CONFIG.earliestInstant)
    val end = timeRange._2
    val versionsInRange = history.versions.range(begin,end)
    if(!versionsInRange.isEmpty){
      stats.numHistoriesWithVersionPresent+=1
    }
    stats.numVersionsPresentSum += versionsInRange.size
    stats.hashedDistinctValues.addAll(versionsInRange.flatMap(_._2.values.map(_.hashCode)))
  }

  def toKryoSerializableColumnHistory  = {
    val hist = history.versions.values.map(acv => acv.asInstanceOf[ColumnVersion].toKryoSerializableColumnHistory).toIndexedSeq
    val list = new util.ArrayList[KryoSerializableColumnVersion]()
    hist.foreach(list.add(_))
    val res = new KryoSerializableColumnHistory()
    res.id =id
    res.tableId=tableId
    res.pageID=pageID
    res.pageTitle=pageTitle
    res.hist=list
    res
  }

  def compositeID = s"$pageID--$tableId--$id"

  def allValues:Set[String] = history
    .versions
    .values
    .toSet
    .flatMap((t:AbstractColumnVersion[String]) => t.values)


  def versionAt(v: Instant) = {
    if(history.versions.contains(v))
      history.versions(v)
    else {
      val option = history
        .versions
        .maxBefore(v)
      option.getOrElse( (AbstractColumnVersion.INITIALEMPTYID,ColumnVersion.COLUMN_DELETE(AbstractColumnVersion.INITIALEMPTYID,v.toString)))
        ._2
    }
  }

}
object OrderedColumnHistory {

  def readFromFiles(sourceDir:File) = {
    ColumnHistory
      .iterableFromJsonObjectPerLineDir(sourceDir,true)
      .map(ch => ch.asOrderedHistory)
  }

  def fromKryoSerializableColumnHistory(kryo:KryoSerializableColumnHistory) = {
    val versionMap = kryo.hist.asScala
      .map(k => {
        val version = ColumnVersion.fromKryoSerializableColumnHistory(k)
        (version.timestamp,version)
      })
    val versionList = new OrderedColumnVersionList(collection.mutable.TreeMap[Instant,ColumnVersion]() ++ versionMap)
    new OrderedColumnHistory(kryo.id,
      kryo.tableId,
      kryo.pageID,
      kryo.pageTitle,
      versionList)
  }

}