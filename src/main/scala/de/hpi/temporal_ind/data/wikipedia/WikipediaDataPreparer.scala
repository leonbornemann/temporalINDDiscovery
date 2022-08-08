package de.hpi.temporal_ind.data.wikipedia

import de.hpi.temporal_ind.data.column.data.original.ColumnHistory
import de.hpi.temporal_ind.util.Util
import org.jsoup.Jsoup

import java.time.Instant
import java.time.temporal.ChronoUnit

class WikipediaDataPreparer {


  def removeTableHeader(tableHistory: TableHistory):TableHistory = {
    tableHistory.withoutHeader
  }

  def extractColumnLineagesFromTableHistory(tableHistory: TableHistory) = {
    tableHistory
      .extractColumnHistories
      .map(ch => ch.transformValueset(removeHTMLBoilerplateAndExtractLinkTargets))
      .map(ch => ch.transformHeader(removeHTMLBoilerplateAndExtractLinkTargets))
      .map(ch => ch.transformValueset(unifyNullSymbols))
      .map(ch => filterVandalism(ch))
      .map(ch => removeDuplicateVersions(ch))
      .filter(ch => !mostlyNumeric(ch))
  }

  def removeHTMLBoilerplateAndExtractLinkTargets(strings:Set[String]):Set[String] = {
    strings.map(s => {
      val doc = Jsoup.parse(s)
      //get link targets:
      val elemIt =doc.select("a").iterator()
      while(elemIt.hasNext){
        val elem = elemIt.next()
        if(elem.hasAttr("title")){
          val pageTitle = elem.attr("title")
          elem.text(pageTitle)
        } else if(elem.hasAttr("href")){
          val targetPage = elem.attr("href")
          elem.text(targetPage)
        }
      }
      doc.text()
    })
  }

  def durationLongEnough(revisionDate: Instant, revisionDate1: Instant): Boolean = {
    val days = ChronoUnit.DAYS.between(revisionDate,revisionDate1)
    days >=1
  }

  def removeDuplicateVersions(ch:ColumnHistory) = {
    val withIndex = ch
      .columnVersions
      .zipWithIndex
    val withOutDuplicates = withIndex
      .withFilter{case (cv,i) => i==0 || (cv.values!=withIndex(i-1)._1.values || cv.isDelete!=withIndex(i-1)._1.isDelete) }
      .map(_._1)
    ColumnHistory(ch.id,ch.tableId,ch.pageID,ch.pageTitle,withOutDuplicates)
  }

  def filterVandalism(ch: ColumnHistory) = {
    val withoutVandalism = ch.columnVersions
      .zipWithIndex
      .withFilter{case (cv,i) => {
        i==ch.columnVersions.size-1 || durationLongEnough(cv.timestamp,ch.columnVersions(i+1).timestamp)
      }}
      .map(_._1)
    ColumnHistory(ch.id,ch.tableId,ch.pageID,ch.pageTitle,withoutVandalism)
  }

  def unifyNullSymbols(strings: Set[String]):Set[String] = {
    strings.map(s => if(GLOBAL_CONFIG.NULL_VALUE_EQUIVALENTS.contains(s)) GLOBAL_CONFIG.CANONICAL_NULL_VALUE else s)
  }

  def isNumeric(values: Set[String]): Boolean = {
    values.forall(s => s.trim.matches(Util.numberRegex))
  }

  def mostlyNumeric(ch:ColumnHistory) = {
    //if 50% of the alive time the column is purely numeric we consider this a numeric column
    val numericTime = ch.nonDeleteVersionsWithAliveTime(GLOBAL_CONFIG.lastInstant)
      .map{case (cv,aliveTime) => if(isNumeric(cv.values)) aliveTime else 0L}
      .sum
    val totalTime = ch.nonDeleteVersionsWithAliveTime(GLOBAL_CONFIG.lastInstant)
      .map(_._2)
      .sum
    numericTime/totalTime.toDouble>0.5
  }
}
