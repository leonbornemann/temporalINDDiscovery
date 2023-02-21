package de.hpi.temporal_ind.data.column.data.original

import de.hpi.temporal_ind.data.column.data.AbstractColumnVersion

import java.time.Instant

class PeekableIterator[A](iterator: Iterator[A]) extends Iterator[A] {

  var nextOrEmpty:Option[A] = iterator.nextOption()
  override def hasNext: Boolean = nextOrEmpty.isDefined

  override def next(): A = {
    val res = nextOrEmpty.get
    nextOrEmpty = iterator.nextOption()
    res
  }

  //peeks at the next element without removing it
  def peek:Option[A] = {
    nextOrEmpty
  }
}


