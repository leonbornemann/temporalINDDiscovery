package de.hpi.temporal_ind

import de.hpi.temporal_ind.data.ind.ExponentialDecayWeightFunction
import de.hpi.temporal_ind.data.wikipedia.GLOBAL_CONFIG

import java.time.temporal.ChronoUnit

object Test extends App {
  GLOBAL_CONFIG.setSettingsForDataSource("wikipedia")
  println(GLOBAL_CONFIG.totalTimeInDays)
  val function = new ExponentialDecayWeightFunction(0.99977126471818,ChronoUnit.DAYS)
  val t = GLOBAL_CONFIG.earliestInstant.plus(GLOBAL_CONFIG.totalTimeInDays/2,ChronoUnit.DAYS)
  println(function.weight(t))

  val ms= scala.collection.mutable.MultiSet[Int]()
  ms ++= Seq(1,1,2)
  println(ms.toSet)
  println(ms)
  ms --= Seq(1,2,3)
  println(ms)
  println(ms.contains(1))
  println(ms.contains(2))
  println(ms.toSet)

  val start = 1
  val end = 10
  val a = 0.8

  def integral(start:Int,end:Int,a:Double) = {
    1.0 / Math.log(a) * (math.pow(a,end)-math.pow(a,start))
  }

  def infiniteIntegral(a:Double,t:Int) = {
    1.0 / Math.log(a) * Math.pow(a,t)
  }
  def exponentialDecay(a:Double,timestamp:Int) = {
    math.pow(a,timestamp)
  }

  def partialGeometricSum(start:Int, endExclusive:Int, a:Double) = {
    val untilEnd = (1-math.pow(a,endExclusive)) / (1-a)
    val untilStart = (1-math.pow(a,start)) / (1-a)
    untilEnd-untilStart
  }

  def sumOfTimestamps(start:Int, endExclusive:Int, a:Double) = {
    val value = (start until endExclusive)
      .map(i => (i,exponentialDecay(a, i)))
    value
      .foreach(println)
    value
      .map(_._2)
      .sum
  }


  println("Sum variant",sumOfTimestamps(start,end,a))
  //println(infiniteIntegral(a,end))
  println("Integral Variant",integral(start,end,a))
  println("Geometric Sum",partialGeometricSum(start,end,a))

  def temporalDecay(start:Int,end:Int,a:Double,maxTimestamp:Int) = {
    (1-math.pow(a,maxTimestamp-start)) / (1-a) - (1-math.pow(a,maxTimestamp-end)) / (1-a)
  }

  def temporalDecayV2(start: Int, end: Int, a: Double, maxTimestamp: Int) = {
    math.pow(a, maxTimestamp) * (math.pow(a, -end) - math.pow(a, -start)) / (1 - a)
  }

  println("Temporal Weight Decayed",5,10,20,temporalDecay(5,10,a,20))
  println("Temporal Weight Decayed",6,11,20,temporalDecay(6,11,a,20))
  println("Temporal Weight Decayed",19,20,20,temporalDecay(19,20,a,20))

  println("Temporal Weight Decayed", 5, 10, 20, temporalDecayV2(5, 10, a, 20))
  println("Temporal Weight Decayed", 6, 11, 20, temporalDecayV2(6, 11, a, 20))
  println("Temporal Weight Decayed", 19, 20, 20, temporalDecayV2(19, 20, a, 20))
  println("Temporal Weight Decayed", 11, 12, 20, temporalDecayV2(11, 12, a, 20))
}
