package de.hpi.temporal_ind.data.column.data.encoded

import de.hpi.temporal_ind.data.column.data.{AbstractColumnVersion, AbstractOrderedColumnHistory}
import de.hpi.temporal_ind.data.column.data.original.{ColumnVersion, OrderedColumnVersionList}

import java.time.Instant

class OrderedEncodedColumnHistory(val id: String,
                           val tableId: String,
                           val pageID: String,
                           val pageTitle: String,
                           val history: OrderedEncodedColumnVersionList) extends AbstractOrderedColumnHistory[Long] {

  def versionAt(v: Instant) = {
    if(history.versions.contains(v))
      history.versions(v)
    else {
      val option = history
        .versions
        .maxBefore(v)
      option.getOrElse( (AbstractColumnVersion.INITIALEMPTYID,ColumnVersionEncoded.COLUMN_DELETE(AbstractColumnVersion.INITIALEMPTYID,v.toString)))
        ._2
    }
  }

}
