import de.hpi.temporal_ind.data.column.data.original.{ColumnVersion, OrderedColumnHistory, OrderedColumnVersionList}
import de.hpi.temporal_ind.data.wikipedia.GLOBAL_CONFIG

import java.io.File
import java.time.{Duration, Instant}
import scala.io.Source

class TemporalINDTestCase(val name:String,
                          val lhs:OrderedColumnHistory,
                          val rhs:OrderedColumnHistory,
                          val shouldBeValidForVariant:IndexedSeq[Boolean]) {
}

object TemporalINDTestCase {

  val earliestDate = Instant.parse("2001-01-01T00:00:01Z")

  def readFromFile(file: File,filterDuplicateVersions:Boolean=true) = {
    val name = file.getName
    val lines = Source.fromFile(file)
      .getLines()
      .toIndexedSeq
    val header = lines.head.split("\t")
    val lastDataIndex = header.indexOf("Strict Valid")-1
    val timeaxis = (0 until lastDataIndex).map(i => earliestDate.plus(Duration.ofDays(i)))
    val dataLines = lines
      .tail
      .map(s => s.split("\t"))
    val histories = dataLines
      .map(tokens => {
        val columnID = tokens(0)
        val values = tokens.slice(1,lastDataIndex+1)
          .map(_.split(",").map(_.trim).toSet)
          .toIndexedSeq
        assert(timeaxis.size==values.size)
        var res = (collection.mutable.TreeMap[Instant,ColumnVersion]() ++ timeaxis.zip(values)
          .map{case (t,v) => {
            val values = if(v.size==1 && v.head=="Ø") Set[String]() else v
            (t,new ColumnVersion(t.toString,t.toString,values,v.isEmpty))
          }})
          .toIndexedSeq
        if(filterDuplicateVersions){
          res = res
            .zipWithIndex
            .filter{case ((t,v),i) => i==0 || v.values!=res(i-1)._2.values}
            .map(_._1)
        }
        val asMap = collection.mutable.TreeMap[Instant,ColumnVersion]() ++ res
        val list = new OrderedColumnVersionList(asMap)
        new OrderedColumnHistory(columnID,"dummyTAbleID","dummyPageID","dummyPageTitle",list)
      })
    assert(histories.size==2)
    val shouldRemainValid = dataLines.head.slice(lastDataIndex+1,lines.head.size)
      .map(_.toBoolean)
    GLOBAL_CONFIG.earliestInstantWikipedia = earliestDate
    GLOBAL_CONFIG.latestInstantWikipedia = earliestDate.plus(Duration.ofDays(lastDataIndex-1))
    new TemporalINDTestCase(name,histories(0),histories(1),shouldRemainValid)
  }

}
