package de.hpi.temporal_ind.discovery

import de.hpi.temporal_ind.data.ind.{ConstantWeightFunction, TimestampWeightFunction}
import de.hpi.temporal_ind.data.wikipedia.GLOBAL_CONFIG

case class TINDParameters(absoluteEpsilon: Double, absDeltaInNanos: Long, omega: TimestampWeightFunction) {


}
