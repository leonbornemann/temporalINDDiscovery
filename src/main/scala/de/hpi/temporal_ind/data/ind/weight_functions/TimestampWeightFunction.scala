package de.hpi.temporal_ind.data.ind.weight_functions

import java.time.Instant

abstract class TimestampWeightFunction {
  def getIntervalOfWeight(start: Instant, weight: Double):(Instant, Instant)


  def weight(t:Instant):Double

  def weight(startInclusive:Instant,endExclusive:Instant):Double

  //def summedWeightNanos(startInclusive: Instant, endExclusive: Instant):Double
}

object TimestampWeightFunction {

  def create(functionName:String,alpha:Option[Double]) = {
    functionName.toLowerCase match {
      case "constant" => new ConstantWeightFunction()
      case "expdecay" => {
        if(alpha.isEmpty)
          throw new IllegalArgumentException("exponential decay function requires alpha paramter")
        new ExponentialDecayWeightFunction(alpha.get)
      }
      case _ => throw new IllegalArgumentException(s"timestamp weighting function with name $functionName is unknown")
    }
  }
}
