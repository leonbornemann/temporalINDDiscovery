import de.hpi.temporal_ind.data.column.data.original.{ColumnVersion, OrderedColumnHistory, OrderedColumnVersionList}
import de.hpi.temporal_ind.util.Util

import java.time.{Instant, ZoneId}
import java.util.Locale

object TestUtilMethods {

  def toHistory(value: Map[Int, Set[String]]) ={
    //class OrderedColumnHistory(val id: String,
    //                           val tableId: String,
    //                           val pageID: String,
    //                           val pageTitle: String,
    //                           val history: OrderedColumnVersionList)
    new OrderedColumnHistory("","","","",new OrderedColumnVersionList(collection.mutable.TreeMap[Instant,ColumnVersion]() ++ value.map(t => {
      val date = toInstant(t._1)
      val dateAsString = Util.wikipediaDateTimeFormatter.withZone(ZoneId.of("UTC")).format(date)
      (date, ColumnVersion("", dateAsString, t._2,None,None, false))
    })))
  }

  def toInstant(nanos:Int) = {
    Instant.ofEpochSecond(0L, nanos)
  }

}
