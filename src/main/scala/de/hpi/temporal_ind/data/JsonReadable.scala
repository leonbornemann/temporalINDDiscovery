package de.hpi.temporal_ind.data

import com.typesafe.scalalogging.StrictLogging
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods.parse

import java.io.{File, FileInputStream}
import scala.io.Source


trait JsonReadable[T <: AnyRef] extends StrictLogging{

  implicit def formats = (DefaultFormats.preservingEmptyValues)


  def fromJsonString(json: String)(implicit m: Manifest[T]) = {
    parse(json).extract[T]
  }

  def fromJsonFile(path: String)(implicit m: Manifest[T]) = {
    //val string = Source.fromFile(path).getLines().mkString("\n")
    val file = new FileInputStream(new File(path))
    val json = parse(file)
    json.extract[T]
  }

  def iterableFromJsonObjectPerLineFile(path: String,
                                        logAfterCompletion:Boolean=false,
                                        fileNumber:Int= -1,
                                        totalFileCount:Int= -1)(implicit m: Manifest[T]) = {
    new JsonObjectPerLineFileIterator(path,logAfterCompletion,fileNumber,totalFileCount)(m)
  }

  def iterableFromJsonObjectPerLineFiles(files: IndexedSeq[File], logFileProgress: Boolean) (implicit m: Manifest[T]) = {
    val fileCount = files.size
    val iterators = files
      .zipWithIndex
      .map{case (f,i) => iterableFromJsonObjectPerLineFile(f.getAbsolutePath,logFileProgress,i+1,fileCount)}
    iterators.foldLeft(Iterator[T]())(_ ++ _)
  }

  def iterableFromJsonObjectPerLineDir(dir: File, logFileProgress:Boolean)(implicit m: Manifest[T]) = {
    iterableFromJsonObjectPerLineFiles(dir.listFiles(),logFileProgress)(m)
  }

  def fromJsonObjectPerLineFile(path: String)(implicit m: Manifest[T]): collection.Seq[T] = {
    val result = scala.collection.mutable.ArrayBuffer[T]()
    Source.fromFile(path).getLines()
      .foreach(l => {
        result.addOne(fromJsonString(l))
      })
    result
  }

  class JsonObjectPerLineFileIterator(path: String,
                                      logAfterCompletion:Boolean=false,
                                      fileNumber:Int= -1,
                                      totalFileCount:Int= -1)(implicit m: Manifest[T]) extends Iterator[T] {
    val it = Source.fromFile(path).getLines()

    override def hasNext: Boolean = it.hasNext

    override def next(): T = {
      val res = fromJsonString(it.next())
      if(!hasNext && logAfterCompletion) {
        logger.debug(s"Finished File $path ($fileNumber / $totalFileCount)")
      }
      res
    }
  }
}
