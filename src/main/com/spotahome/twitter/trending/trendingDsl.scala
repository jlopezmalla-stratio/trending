package com.spotahome.twitter.trending

import org.apache.commons.lang3.StringUtils

import scala.annotation.tailrec
import org.joda.time.DateTime

class PrintFunction (trendingTopic: Seq[(String, Int)]) {

  private def trendingToString(actualTrendingTopic: Seq[((String, Int), Int)],
                               previousTrendingTopic: Seq[((String, Int), Int)],
                               maxHashTagSize: Int): String = {
    val maxValue = 5
    val maxIndex = 8

    actualTrendingTopic.map { case ((tweet, value), index) => {
      val previousPosition = previousTrendingTopic.find { case ((oldTweet, _), _) => tweet == oldTweet } match {
        case Some((_, oldIndex)) => {
          val upwardDownward = oldIndex - index
          if (upwardDownward > 0) s"↑($upwardDownward)"
          else if (upwardDownward < 0) s"↓(${-1 * upwardDownward})"
          else "="
        }
        case _ => "new"
      }


      s"|${StringUtils.rightPad(index.toString, maxIndex)}" +
        s"|${StringUtils.rightPad(tweet, maxHashTagSize)}" +
        s"|${StringUtils.rightPad(value.toString, maxValue)}" +
        s"|${StringUtils.rightPad(previousPosition, 15)}|" }
    }.mkString("\n")
  }

  def printTrendingTopic(initTime: DateTime,
                         endTime: DateTime,
                         previousTrending: Seq[(String, Int)]): Unit = {

    val previousTrendingTopic = previousTrending.zipWithIndex
    val actualTrendingTopic = trendingTopic.zipWithIndex
    val maxHashTag = actualTrendingTopic.map(_._1._1.size).max

    println(s"Tweets from ${initTime.toString("yyyy-MM-dd hh.mm.ss")} to " +
      s"${endTime.toString("yyyy-MM-dd hh.mm.ss")}")
    println("---------------------------------------------------")
    println(s"|position|${StringUtils.rightPad("HashTag", maxHashTag)}|" +
      s"${StringUtils.rightPad("Count", 5)}|Upward/Downward|")
    println(trendingToString(actualTrendingTopic, previousTrendingTopic, maxHashTag))

  }
}


class CalculationFunction (tweets: Seq[(DateTime, String)]) {

  def trendingTopic: Seq[(String, Int)] = {
    import TrendingDsl._
    tweets.groupBy{case (_, hashTag) => hashTag}
      .mapValues(_.size)
      .toSeq.takeTrending()
  }
}

class SortedFunction (tweetsCounter: Seq[(String, Int)]) {

  def takeTrending(maximum: Int = 10): Seq[(String, Int)] = {
    @tailrec
    def takeTrending(maximum: Int,
                     topics: Seq[(String, Int)] = Seq.empty[(String, Int)],
                     result: Seq[(String, Int)] = Seq.empty[(String, Int)]): Seq[(String, Int)] = {
      if (topics.isEmpty) result
      else {
        val head = topics.head
        val resultHead = if(result.headOption.isDefined) result.head._2 else 0
        val newResult =  if (result.size < 10 || head._2 >= resultHead) {
          (result ++ Seq(head)).sortBy { case (_, counter) => counter }.takeRight(10)
        } else result
        takeTrending(10, topics.tail, newResult)
      }
    }

    takeTrending(maximum, tweetsCounter).sortBy{case (_, counter) => - counter}
  }
}

trait TrendingDsl {

  implicit def calculationFunction(tweetsQueue: Seq[(DateTime, String)]): CalculationFunction = new CalculationFunction(tweetsQueue)

  implicit def sortedFunction(tweetsCounter: Seq[(String, Int)]): SortedFunction = new SortedFunction(tweetsCounter)

  implicit def printFunction(trendingTopic: Seq[(String, Int)]): PrintFunction = new PrintFunction(trendingTopic)
}

object TrendingDsl extends TrendingDsl