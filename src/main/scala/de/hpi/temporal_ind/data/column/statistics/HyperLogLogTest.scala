package de.hpi.temporal_ind.data.column.statistics

import com.google.zetasketch.HyperLogLogPlusPlus
import de.hpi.temporal_ind.data.column.data.IncrementalIndexedColumnHistories

import java.io.{File, PrintWriter}
import java.lang
import scala.util.Random

object HyperLogLogTest extends App {
  val inputDir = args(0)
  val outputFile = new PrintWriter(args(1))
  val index = new IncrementalIndexedColumnHistories(new File(inputDir))
  outputFile.println("estimatedOverlap,trueOverlap")
  (0 until 1000).foreach(i => {
    val a = index.getRandomColumnHistory()
    val b = index.getRandomColumnHistory()
    val estimatedOverlap = a.hyperLogLogOverlapOfUnionedValuesets(b)
    val trueOverlap = a.overlapOfUnionedValuesets(b)
    outputFile.println(s"$estimatedOverlap,$trueOverlap")
  })
  outputFile.close()


//  val hll:HyperLogLogPlusPlus[String] = new HyperLogLogPlusPlus.Builder().buildForStrings();
//  val hll2:HyperLogLogPlusPlus[String] = new HyperLogLogPlusPlus.Builder().buildForStrings();
//  val numStrings = 30
//  val stringLength = (5 until 100)
//  (0 until numStrings).foreach(_ => {
//    val length1 = Random.nextInt(stringLength.size)
//    val string1 = Random.alphanumeric.filter(_.isLetter).take(length1).toIndexedSeq.mkString
//    val length2 = Random.nextInt(stringLength.size)
//    val string2 = Random.alphanumeric.filter(_.isLetter).take(length2).toIndexedSeq.mkString
//    hll.add(string1)
//    hll2.add(string2)
//  })
//  private val sizeA: lang.Long = hll.result()
//  println(sizeA)
//  private val sizeB: lang.Long = hll2.result()
//  println(sizeB)
//  hll.merge(hll2)
//  private val union: lang.Long = hll.result()
//  println(union)
//  val intersection = sizeA+sizeB-union
//  println(intersection)



}
