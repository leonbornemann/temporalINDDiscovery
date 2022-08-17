import de.hpi.temporal_ind.data.column.data.TimeIntervalSequence
import de.hpi.temporal_ind.data.wikipedia.GLOBAL_CONFIG
import org.scalatest.flatspec.AnyFlatSpec

class TimeIntervalSequenceTest extends AnyFlatSpec{

  def getIntervalSequence(value: IndexedSeq[(Int, Int)]) = {
    new TimeIntervalSequence(value.map(t => (TestUtilMethods.toInstant(t._1),TestUtilMethods.toInstant(t._2))))
  }

  "Time Interval Sequence Intersection" should "work correctly" in {
    GLOBAL_CONFIG.lastInstant = TestUtilMethods.toInstant(30)
    val intervals1 = getIntervalSequence(IndexedSeq(
      (1,2),
      (2,6),
      (10,17)
    ))
    val intervals2 = getIntervalSequence(IndexedSeq(
      (2,6)
    ))
    val intervals3 = getIntervalSequence(IndexedSeq(
      (4,8)
    ))
    val intervals4 = getIntervalSequence(IndexedSeq(
      (1,4)
    ))
    val intervals5 = getIntervalSequence(IndexedSeq(
      (1,4),
      (7,8),
      (10,14)
    ))
    val intervals6 = getIntervalSequence(IndexedSeq(
      (7,8)
    ))
    val intervals7 = getIntervalSequence(IndexedSeq())
    val intervals8 = getIntervalSequence(IndexedSeq((0,25)))
    assert(intervals1.intersect(intervals1).summedDurationNanos == 12)
    assert(intervals1.intersect(intervals2).summedDurationNanos == 4)
    assert(intervals1.intersect(intervals3).summedDurationNanos == 2)
    assert(intervals1.intersect(intervals4).summedDurationNanos == 3)
    assert(intervals1.intersect(intervals5).summedDurationNanos == 7)
    assert(intervals1.intersect(intervals6).summedDurationNanos == 0)
    assert(intervals1.intersect(intervals7).summedDurationNanos == 0)
    assert(intervals1.intersect(intervals8).intervals == intervals1.intervals)
  }

  "Time Interval Sequence Union" should "work correctly" in {
    GLOBAL_CONFIG.lastInstant = TestUtilMethods.toInstant(30)
    val intervals1 = getIntervalSequence(IndexedSeq(
      (1,2),
      (2,6),
      (10,17)
    ))
    val intervals2 = getIntervalSequence(IndexedSeq(
      (2,6)
    ))
    val intervals3 = getIntervalSequence(IndexedSeq(
      (4,8)
    ))
    val intervals4 = getIntervalSequence(IndexedSeq(
      (1,4)
    ))
    val intervals5 = getIntervalSequence(IndexedSeq(
      (1,4),
      (7,8),
      (10,14)
    ))
    val intervals6 = getIntervalSequence(IndexedSeq(
      (1,20)
    ))
    val intervals7 = getIntervalSequence(IndexedSeq(
      (1,12),
      (13,25)
    ))
    assert(intervals1.union(intervals1).summedDurationNanos == 12)
    assert(intervals1.union(intervals2).summedDurationNanos == 12)
    assert(intervals1.union(intervals3).summedDurationNanos == 14)
    assert(intervals1.union(intervals4).summedDurationNanos == 12)
    assert(intervals1.union(intervals5).summedDurationNanos == 13)
    assert(intervals1.union(intervals6).summedDurationNanos == 19)
    assert(intervals1.union(intervals7).summedDurationNanos == 24)
  }

}
