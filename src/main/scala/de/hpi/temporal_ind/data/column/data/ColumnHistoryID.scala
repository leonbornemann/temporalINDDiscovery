package de.hpi.temporal_ind.data.column.data

import de.hpi.temporal_ind.data.{JsonReadable, JsonWritable}

case class ColumnHistoryID(pageID:String,tableID:String,columnID:String) extends JsonWritable[ColumnHistoryID]

object ColumnHistoryID extends JsonReadable[ColumnHistoryID]
