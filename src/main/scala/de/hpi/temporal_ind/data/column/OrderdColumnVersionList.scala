package de.hpi.temporal_ind.data.column

import java.time.Instant
import scala.collection.mutable

class OrderdColumnVersionList(val versions : mutable.TreeMap[Instant, ColumnVersion]) {

}
