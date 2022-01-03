package de.hpi.temporal_ind.data.column

import de.hpi.temporal_ind.data.{JsonReadable, JsonWritable}

import scala.collection.mutable.ArrayBuffer

case class ColumnHistoryEncoded(id: String, tableId: String, pageID: String, pageTitle: String, value: ArrayBuffer[ColumnVersionEncoded]) extends JsonWritable[ColumnHistoryEncoded] {

}
object ColumnHistoryEncoded extends JsonReadable[ColumnHistoryEncoded]
