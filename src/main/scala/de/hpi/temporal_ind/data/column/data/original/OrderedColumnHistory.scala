package de.hpi.temporal_ind.data.column.data.original

import de.hpi.temporal_ind.data.column.data.{AbstractColumnVersion, AbstractOrderedColumnHistory}

import java.io.File
import java.time.Instant

class OrderedColumnHistory(val id: String,
                           val tableId: String,
                           val pageID: String,
                           val pageTitle: String,
                           val history: OrderedColumnVersionList) extends AbstractOrderedColumnHistory[String] {
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

}