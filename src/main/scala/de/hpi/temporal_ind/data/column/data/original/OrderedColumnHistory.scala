package de.hpi.temporal_ind.data.column.data.original

import de.hpi.temporal_ind.data.column.data.{AbstractColumnVersion, AbstractOrderedColumnHistory}

import java.io.File
import java.time.Instant
import java.util
import scala.jdk.CollectionConverters.{ListHasAsScala, SeqHasAsJava}

class OrderedColumnHistory(val id: String,
                           val tableId: String,
                           val pageID: String,
                           val pageTitle: String,
                           val history: OrderedColumnVersionList) extends AbstractOrderedColumnHistory[String] with Serializable{
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