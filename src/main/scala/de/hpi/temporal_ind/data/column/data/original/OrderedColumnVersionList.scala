package de.hpi.temporal_ind.data.column.data.original

import de.hpi.temporal_ind.data.column.data.{AbstractColumnVersion, AbstractOrderdColumnVersionList}

import java.time.Instant
import scala.collection.mutable

class OrderedColumnVersionList(val versions : mutable.TreeMap[Instant, _ <: AbstractColumnVersion[String]]) extends AbstractOrderdColumnVersionList[String]{

}
