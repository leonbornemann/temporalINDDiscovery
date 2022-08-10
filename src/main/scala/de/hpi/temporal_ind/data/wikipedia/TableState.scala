package de.hpi.temporal_ind.data.wikipedia

import de.hpi.temporal_ind.data.column.data.original.ColumnVersion

case class TableState(columns:IndexedSeq[ColumnVersion], artificialHeaders:IndexedSeq[String]){
  assert(columns.size==artificialHeaders.size)
}
