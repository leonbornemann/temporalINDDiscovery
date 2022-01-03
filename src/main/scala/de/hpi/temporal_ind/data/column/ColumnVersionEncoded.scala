package de.hpi.temporal_ind.data.column

import de.hpi.temporal_ind.data.{JsonReadable, JsonWritable}

case class ColumnVersionEncoded(revisionID: String, revisionDate: String, values: Set[Long],columnNotPresent:Boolean) extends JsonWritable[ColumnVersionEncoded]{

}
object ColumnVersionEncoded extends JsonReadable[ColumnVersionEncoded]
