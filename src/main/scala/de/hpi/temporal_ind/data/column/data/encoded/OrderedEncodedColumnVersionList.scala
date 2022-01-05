package de.hpi.temporal_ind.data.column.data.encoded

import de.hpi.temporal_ind.data.column.data.{AbstractColumnVersion, AbstractOrderdColumnVersionList}

import java.time.Instant
import scala.collection.mutable

class OrderedEncodedColumnVersionList(val versions : mutable.TreeMap[Instant, AbstractColumnVersion[Long]]) extends AbstractOrderdColumnVersionList[Long]{

}
