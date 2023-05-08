package de.hpi.temporal_ind.data.attribute_history.data.traversal

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


