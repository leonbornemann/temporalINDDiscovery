package de.hpi.temporal_ind.data.wikipedia

import de.hpi.temporal_ind.data.attribute_history.data.original.ColumnVersion
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
  def getTableState(filterExpanded: Boolean) = {
    val columnsToInclude = if(filterExpanded) cells(0).map(!_.wasExpanded) else List.fill(cells(0).size)(true).toIndexedSeq
    val colCount = columnsToInclude.filter(b => b).size
    val headers = if(hasHeader) header(columnsToInclude).map(Some(_)) else List.fill(colCount)(None)
    val artificialHeaders = artificialColumnHeaders
      .zip(columnsToInclude)
      .filter(_._2)
      .map(_._1)
    val columnVersions = getColumnsWithColPosition(columnsToInclude)
      .zip(headers)
      .map{case ((values,position),header) => ColumnVersion(revisionID, revisionDate, values.toSet,header,Some(position),false)}
    TableState(columnVersions,artificialHeaders)
  }

  def header(columnsToInclude:IndexedSeq[Boolean]) = {
    assert(columnsToInclude.size==cells.head.size)
    cells.head
      .zip(columnsToInclude)
      .withFilter(_._2)
      .map(_._1.content)
  }

  def timestamp = Util.instantFromWikipediaDateTimeString(revisionDate)

  def hasHeader = cells.size>0

  def withOutHeader = {
    //just remove the first row for now
    val newCells = if(cells.size>0) cells.tail else cells
    TableVersion(revisionID,revisionDate,contributor,artificialColumnHeaders,newCells,globalNaturalKeys,localNaturalKeys)
  }

  def getColumn(colIndex: Int) = (0 until nrows).
    map(rowIndex => {
      cells(rowIndex)(colIndex).content
    })

  def getColumnsWithColPosition(columnsToInclude:IndexedSeq[Boolean]) = {
    var colPos = 0
    val res = collection.mutable.ArrayBuffer[(IndexedSeq[String],Int)]()
    columnsToInclude
      .zipWithIndex
      .withFilter(_._1)
      .foreach{case (b,colIndex) => {
        res.append((getColumn(colIndex),colPos))
        colPos+=1
      }}
    res.toIndexedSeq
  }

  def get_table_cell(ir: Int, ic: Int) = cells(ir)(ic)

  def nrows = cells.size

  def ncols = artificialColumnHeaders.size

}
