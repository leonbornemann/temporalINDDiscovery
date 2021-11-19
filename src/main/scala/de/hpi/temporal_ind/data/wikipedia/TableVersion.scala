package de.hpi.temporal_ind.data.wikipedia

import de.hpi.temporal_ind.util.Util
import org.jsoup.Jsoup

case class TableVersion(revisionID: String,
                        revisionDate: String,
                        contributor: Contributor,
                        artificialColumnHeaders: IndexedSeq[String],
                        cells: IndexedSeq[IndexedSeq[TableCell]],
                        globalNaturalKeys: IndexedSeq[String],
                        localNaturalKeys: IndexedSeq[String],
                       ) {
  def timestamp = Util.instantFromWikipediaDateTimeString(revisionDate)

  def withOutHeader = {
    //just remove the first row for now
    val newCells = if(cells.size>0) cells.tail else cells
    TableVersion(revisionID,revisionDate,contributor,artificialColumnHeaders,newCells,globalNaturalKeys,localNaturalKeys)
  }

  def getColumn(colIndex: Int) = (0 until nrows).
    map(rowIndex => {
      cells(rowIndex)(colIndex).content
    })

  def getColumns() = {
    (0 until ncols).map(colIndex => getColumn(colIndex))
  }

  def get_table_cell(ir: Int, ic: Int) = cells(ir)(ic)

  def nrows = cells.size

  def ncols = artificialColumnHeaders.size

}
