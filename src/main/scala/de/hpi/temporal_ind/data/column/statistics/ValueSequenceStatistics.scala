package de.hpi.temporal_ind.data.column.statistics

import scala.collection.mutable.ArrayBuffer

case class ValueSequenceStatistics(values: ArrayBuffer[Double]) {

  def min = if (values.isEmpty) Double.NaN else values.min

  def max = if (values.isEmpty) Double.NaN else values.max

  def mean = if (values.isEmpty) Double.NaN else values.sum / values.size.toDouble

  def median = if (values.isEmpty) Double.NaN else values.sorted.apply(values.size / 2)
}
