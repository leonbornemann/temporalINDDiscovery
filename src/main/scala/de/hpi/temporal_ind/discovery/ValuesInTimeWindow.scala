package de.hpi.temporal_ind.discovery

import java.time.Instant

class ValuesInTimeWindow(val beginInclusive:Instant,
                         val endExclusive:Instant,
                         val values:collection.Set[String]) {

}
