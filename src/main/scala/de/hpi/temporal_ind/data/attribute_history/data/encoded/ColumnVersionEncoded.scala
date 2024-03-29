package de.hpi.temporal_ind.data.attribute_history.data.encoded

import de.hpi.temporal_ind.data.JsonReadable
import de.hpi.temporal_ind.data.attribute_history.data.AbstractColumnVersion
import de.hpi.temporal_ind.data.attribute_history.data.original.ColumnVersion

case class ColumnVersionEncoded(revisionID: String, revisionDate: String, values: Set[Long],columnNotPresent:Boolean) extends AbstractColumnVersion[Long]{
  override def header: Option[String] = ???

  override def position: Option[Int] = ???
}
object ColumnVersionEncoded extends JsonReadable[ColumnVersionEncoded] {
  def COLUMN_DELETE(revisionID: String, revisionDate: String): ColumnVersionEncoded = ColumnVersionEncoded(revisionID,revisionDate,Set(),true)

}
