package de.hpi.temporal_ind.discovery

import de.hpi.temporal_ind.data.GLOBAL_CONFIG
import de.hpi.temporal_ind.data.ind.weight_functions.{ConstantWeightFunction, TimestampWeightFunction}

case class TINDParameters(absoluteEpsilon: Double, absDeltaInNanos: Long, omega: TimestampWeightFunction) {


}
