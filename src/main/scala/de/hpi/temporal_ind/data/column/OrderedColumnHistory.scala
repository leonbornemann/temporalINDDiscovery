package de.hpi.temporal_ind.data.column

import java.time.Instant
import scala.collection.mutable

class OrderedColumnHistory(val id: String,
                           val tableId: String,
                           val pageID: String,
                           val pageTitle: String,
                           val history: OrderdColumnVersionList) {
  def versionsInWindow(lowerInclusive: Instant, upperInclusive: Instant) = history
    .versions
    .rangeImpl(Some(lowerInclusive),Some(upperInclusive.plusNanos(1)))
    .keySet


  def valuesInWindow(lowerInclusive: Instant, upperInclusive: Instant) :Set[String] = {
    val res = history.versions
      .rangeImpl(Some(lowerInclusive), Some(upperInclusive.plusNanos(1)))
      .flatMap(_._2.values)
      .toSet
    if (!history.versions.contains(lowerInclusive)) {
      //add previous version because it lasted until lowerInclusive
      val toAdd = history.versions.maxBefore(lowerInclusive)
      if (toAdd.isEmpty)
        assert(!res.isEmpty)
      toAdd.map(_._2.values).getOrElse(Set()) ++ res
    } else {
      res
    }
  }


  def versionAt(v: Instant):ColumnVersion = {
    if(history.versions.contains(v))
      history.versions(v)
    else {
      val option = history
        .versions
        .maxBefore(v)
      option.getOrElse( (ColumnVersion.INITIALEMPTYID,ColumnVersion.COLUMN_DELETE(ColumnVersion.INITIALEMPTYID,v.toString)))
        ._2
    }
  }

}
