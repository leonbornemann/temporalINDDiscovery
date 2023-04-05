package de.hpi.temporal_ind.data.column.data.original

import de.hpi.temporal_ind.data.column.data.{AbstractColumnVersion, AbstractOrderedColumnHistory}
import de.hpi.temporal_ind.data.wikipedia.GLOBAL_CONFIG
import de.hpi.temporal_ind.discovery.statistics_and_results.TimeSliceStats

import java.io.File
import java.time.Instant
import java.util
import scala.collection.mutable
import scala.jdk.CollectionConverters.{ListHasAsScala, SeqHasAsJava}

class OrderedColumnHistory(val id: String,
                           val tableId: String,
                           val pageID: String,
                           val pageTitle: String,
                           val history: OrderedColumnVersionList) extends AbstractOrderedColumnHistory[String] with Serializable{
  def addPresenceForTimeRanges(timeSliceToOccurrences: mutable.TreeMap[Instant,( Instant, SimpleCounter)]) {
    val it = new PeekableIterator(history.versions.valuesIterator)
    val rangesToAddTo = collection.mutable.HashSet[Instant]()
    while(it.hasNext){
      val cur = it.next()
      val next = it.peek
      if(!cur.isDelete){
        val begin = if(timeSliceToOccurrences.contains(cur.timestamp))
          cur.timestamp
        else
          timeSliceToOccurrences
            .maxBefore(cur.timestamp)
            .map(_._1)
            .getOrElse(timeSliceToOccurrences.head._1)
        val end = if(next.isDefined) timeSliceToOccurrences.maxBefore(next.get.timestamp).get._1.plusNanos(1) else timeSliceToOccurrences.last._1
        val timeSlicesToAddTo = timeSliceToOccurrences
          .range(begin,end)
          .keySet
        rangesToAddTo ++=timeSlicesToAddTo
      }
    }
    rangesToAddTo.foreach(begin => {
      timeSliceToOccurrences(begin)._2.count+=1
    })
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