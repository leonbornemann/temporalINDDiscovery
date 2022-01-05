package de.hpi.temporal_ind.data.column.data.encoded

import de.hpi.temporal_ind.data.JsonReadable
import de.hpi.temporal_ind.data.column.data.AbstractColumnVersion
import de.hpi.temporal_ind.data.column.data.original.ColumnVersion

case class ColumnVersionEncoded(revisionID: String, revisionDate: String, values: Set[Long],columnNotPresent:Boolean) extends AbstractColumnVersion[Long]{


}
object ColumnVersionEncoded extends JsonReadable[ColumnVersionEncoded] {
  def COLUMN_DELETE(revisionID: String, revisionDate: String): ColumnVersionEncoded = ColumnVersionEncoded(revisionID,revisionDate,Set(),true)

}
