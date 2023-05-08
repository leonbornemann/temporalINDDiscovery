package de.hpi.temporal_ind.data.attribute_history.data.encoded

import de.hpi.temporal_ind.data.attribute_history.data.{AbstractColumnVersion, AbstractOrderdColumnVersionList}

import java.time.Instant
import scala.collection.mutable

class OrderedEncodedColumnVersionList(val versions : mutable.TreeMap[Instant, AbstractColumnVersion[Long]]) extends AbstractOrderdColumnVersionList[Long]{

}
